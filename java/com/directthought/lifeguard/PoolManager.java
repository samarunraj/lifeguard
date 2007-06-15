
package com.directthought.lifeguard;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.common.JAXBuddy;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.sqs.Message;
import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.QueueService;
import com.xerox.amazonws.sqs.SQSException;

import com.directthought.lifeguard.jaxb.InstanceStatus;
import com.directthought.lifeguard.jaxb.PoolConfig.ServicePool;

public class PoolManager implements Runnable {
	private static Log logger = LogFactory.getLog(PoolManager.class);

	// configuration items
	private String awsAccessId;
	private String awsSecretKey;
	private String queuePrefix;
	private ServicePool config;

	// runtime data - stuff to save when saving state
	private List<String> busyInstances;
	private List<String> idleInstances;

	/**
	 * This constructs a queue manager.
	 *
	 * @param awsAccessId
	 * @param awsSecretKey
	 * @param queuePrefix
	 */
	public PoolManager(String awsAccessId, String awsSecretKey, String queuePrefix, ServicePool config) {
		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
		this.queuePrefix = queuePrefix;
		this.config = config;
		busyInstances = new ArrayList<String>();
		idleInstances = new ArrayList<String>();
	}

	public void run() {
		try {
			QueueService qs = new QueueService(this.awsAccessId, this.awsSecretKey);
			MessageQueue statusQueue = getQueueOrElse(qs, this.queuePrefix+config.getPoolStatusQueue());
			MessageQueue workQueue = getQueueOrElse(qs, this.queuePrefix+config.getServiceWorkQueue());

			// TODO - don't count loop iterations, track real time passage
			int busyTimer = 0;	// used to track time pool has no idle capacity
			int idleTimer = 0;	// used to track time pool has spare capacity

			// now, loop forever, checking for busy status and checking work queue size
			logger.info("Starting PoolManager for service : "+config.getServiceName());
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
					try {
						InstanceStatus status = JAXBuddy.deserializeXMLStream(InstanceStatus.class,
									new ByteArrayInputStream(msg.getMessageBody().getBytes()));
						// assume we have a change of state, so move instance between busy/idle
						String id = status.getInstanceId();
						if (status.getState().equals("busy")) {
							idleInstances.remove(id);
							busyInstances.add(id);
						}
						else if (status.getState().equals("idle")) {
							busyInstances.remove(id);
							idleInstances.add(id);
						}
						// else, wouldn't parse so ignore this case

						// now, see if were full busy, or somewhat idle
						if (idleInstances.size() == 0) { // busy!
							busyTimer++;
							idleTimer = 0;
						}
						else {
							idleTimer++;
							busyTimer = 0;
						}
					} catch (JAXBException ex) {
						logger.error("Problem parsing instance status!", ex);
					}
					statusQueue.deleteMessage(msg);
					msg = null;
				}
				try {
					int queueDepth = workQueue.getApproximateNumberOfMessages();
					logger.debug("queue depth = "+queueDepth);
					// now, based on busy/idle timers and queue depth, make a call on
					// whether to start or terminate servers
					// TODO
				} catch (SQSException ex) {
					logger.error("Error getting queue depth, Retrying.", ex);
				}
				logger.info("loop bottom");
				try { Thread.sleep(2000); } catch (InterruptedException iex) { }
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

	private void launchInstances(int numToLaunch) {
	}

	private void terminateInstance(String instanceId) {
	}
}
