
package com.directthought.lifeguard;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.sqs.Message;
import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.QueueService;
import com.xerox.amazonws.sqs.SQSException;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.ReservationDescription;

import com.directthought.lifeguard.jaxb.InstanceStatus;
import com.directthought.lifeguard.jaxb.PoolConfig;

public class PoolManager implements Runnable {
	private static Log logger = LogFactory.getLog(PoolManager.class);

	private String awsAccessId;
	private String awsSecretKey;
	private String queuePrefix;
	private PoolConfig config;

	/**
	 * This constructs a queue manager.
	 *
	 * @param awsAccessId
	 * @param awsSecretKey
	 * @param queuePrefix
	 */
	public PoolManager(String awsAccessId, String awsSecretKey, String queuePrefix, PoolConfig config) {
		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
		this.queuePrefix = queuePrefix;
		this.config = config;
	}

	public void run() {
		try {
			QueueService qs = new QueueService(this.awsAccessId, this.awsSecretKey);
			MessageQueue statusQueue = getQueueOrElse(qs, this.queuePrefix+config.getPoolStatusQueue());
			MessageQueue workQueue = getQueueOrElse(qs, this.queuePrefix+config.getServiceWorkQueue());

			// now, loop forever, checking for busy status and checking work queue size
			while (true) {
				Message msg = null;
				try {
					msg = statusQueue.receiveMessage();
				} catch (SQSException ex) {
					logger.error("Error reading message, Retrying.", ex);
				}
				if (msg != null) {	// process status message
					logger.debug("got instance status message");
					// parse it, then deal with it
					msg = null;
				}
				try {
					int queueDepth = workQueue.getApproximateNumberOfMessages();
					logger.debug("queue depth = "+queueDepth);
				} catch (SQSException ex) {
					logger.error("Error getting queue depth, Retrying.", ex);
				}
				logger.info("loop bottom");
			}

		} catch (Throwable t) {
			logger.error(t);
		}
	}

	private MessageQueue getQueueOrElse(QueueService qs, String queueName) {
		MessageQueue ret = null;
		while (ret == null) {
			try {
				ret = qs.getOrCreateMessageQueue(queueName);
			} catch (SQSException ex) {
				logger.error("Error access message queue, Retrying.", ex);
				try { Thread.sleep(1000); } catch (InterruptedException iex) { }
			}
		}
		return ret;
	}
}
