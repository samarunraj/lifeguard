
package com.directthought.lifeguard;

import java.io.ByteArrayInputStream;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.common.JAXBuddy;
import com.xerox.amazonws.sqs.Message;
import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.QueueService;
import com.xerox.amazonws.sqs.SQSException;

import com.directthought.lifeguard.jaxb.WorkStatus;
import com.directthought.lifeguard.util.QueueUtil;

public class StatusLogger implements Runnable {
	private static Log logger = LogFactory.getLog(AbstractBaseService.class);
	private static int RECEIVE_COUNT = 20;
	private String accessId;
	private String secretKey;
	private String queuePrefix;
	private String statusQueueName;
	private StatusSaver saver;
	private boolean keepRunning = true;

	public StatusLogger() {
	}

	public void setAccessId(String id) {
		accessId = id;
	}

	public void setSecretKey(String key) {
		secretKey = key;
	}

	public void setQueuePrefix(String prefix) {
		queuePrefix = prefix;
	}

	public void setStatusQueueName(String name) {
		statusQueueName = name;
	}

	public void setStatusSaver(StatusSaver saver) {
		this.saver = saver;
	}

	public void run() {
		try {
			// connect to queues
			QueueService qs = new QueueService(accessId, secretKey);
			MessageQueue workStatusQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+statusQueueName);
			while (keepRunning) {
				Message [] msgs = null;
				try {
					msgs = workStatusQueue.receiveMessages(RECEIVE_COUNT);
				} catch (SQSException ex) {
					logger.error("Error reading message, Retrying.", ex);
				}
				for (Message msg : msgs) {
					if (!keepRunning) break;	// fast exit
					try {
						WorkStatus status = JAXBuddy.deserializeXMLStream(WorkStatus.class,
									new ByteArrayInputStream(msg.getMessageBody().getBytes()));
						if (!keepRunning) break;	// fast exit
						if (saver != null) {
							saver.workStatus(status); 
						}
					} catch (JAXBException ex) {
						logger.error("Problem parsing instance status!", ex);
					}
					workStatusQueue.deleteMessage(msg);
				}
				try { Thread.sleep(5000); } catch (InterruptedException iex) { }
			}
		} catch (Throwable t) {
			logger.error("Something unexpected happened in the status logger", t);
		}
	}

	public void shutdown() {
		keepRunning = false;
	}
}
