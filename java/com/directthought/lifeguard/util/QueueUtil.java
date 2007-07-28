
package com.directthought.lifeguard.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.QueueService;
import com.xerox.amazonws.sqs.SQSException;

public class QueueUtil {
	private static Log logger = LogFactory.getLog(QueueUtil.class);

	public static MessageQueue getQueueOrElse(QueueService qs, String queueName) {
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

	public static void sendMessageForSure(MessageQueue queue, String message) {
		while (true) {
			try {
				queue.sendMessage(message);
			} catch (SQSException ex) {
				logger.warn("Error sending message, Retrying.");
				try { Thread.sleep(2000); } catch (InterruptedException iex) {}
			}
		}
	}
}
