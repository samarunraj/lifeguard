
package com.directthought.lifeguard;

import java.io.ByteArrayInputStream;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.common.JAXBuddy;
import com.xerox.amazonws.sqs2.Message;
import com.xerox.amazonws.sqs2.MessageQueue;
import com.xerox.amazonws.sqs2.QueueService;
import com.xerox.amazonws.sqs2.SQSException;

import com.directthought.lifeguard.jaxb.WorkStatus;
import com.directthought.lifeguard.util.QueueUtil;

public class StatusLogger implements Runnable {
	private static Log logger = LogFactory.getLog(AbstractBaseService.class);
	private static int RECEIVE_COUNT = 10;
	private String accessId;
	private String secretKey;
	private String secretAccessId;
	private String secretSecretKey;
	private String dblSecretAccessId;
	private String dblSecretSecretKey;
	private String queuePrefix;
	private String statusQueueName;
	private StatusSaver saver;
	private String proxyHost;
	private int proxyPort;
	private boolean keepRunning = true;

	public StatusLogger() {
	}

	public void setAccessId(String id) {
		accessId = id;
	}

	public void setSecretKey(String key) {
		secretKey = key;
	}

	public void setSecretAccessId(String id) {
		secretAccessId = id;
	}

	public void setSecretSecretKey(String key) {
		secretSecretKey = key;
	}

	public void setDblSecretAccessId(String id) {
		dblSecretAccessId = id;
	}

	public void setDblSecretSecretKey(String key) {
		dblSecretSecretKey = key;
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

	public void setProxyHost(String host) {
		proxyHost = host;
	}

	public void setProxyPort(String port) {
		if (!port.trim().equals("")) {
			try {
				proxyPort = Integer.parseInt(port);
			} catch (NumberFormatException ex) {
				logger.error("Could not parse proxy port!", ex);
			}
		}
	}

	public void run() {
		try {
			// connect to queues
			QueueService qs = null;
			if (secretAccessId != null && secretSecretKey != null &&	// using double secret scheme
				dblSecretAccessId != null && dblSecretSecretKey != null &&
				!secretAccessId.equals("") && !secretAccessId.equals("") &&
				!dblSecretAccessId.equals("") && !dblSecretAccessId.equals("")) {
				qs = new QueueService(dblSecretAccessId, dblSecretSecretKey);
			}
			else {
				qs = new QueueService(accessId, secretKey);
			}
			if (!proxyHost.trim().equals("")) {
				qs.setProxyValues(proxyHost, proxyPort);
			}
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
				// sleep, but only if there weren't messages to read
				if (msgs == null || msgs.length == 0) {
					try { Thread.sleep(5000); } catch (InterruptedException iex) { }
				}
			}
		} catch (Throwable t) {
			logger.error("Something unexpected happened in the status logger", t);
		}
	}

	public void shutdown() {
		keepRunning = false;
	}
}
