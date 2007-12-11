
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.Message;
import com.xerox.amazonws.sqs.QueueService;
import com.xerox.amazonws.sqs.SQSUtils;

/**
 * This sample application retrieves (dequeues) a message from the queue specified by
 * the value of the queuename parameter. If successful, it deletes the message from the queue.
 * On error, it retries a number of times.
 */
public class DequeueSample {
    private static Log logger = LogFactory.getLog(DequeueSample.class);

	public static void main( String[] args ) {
		int count = 0;
		try {
			if (args.length < 1) {
				logger.error("usage: DequeueSample <queueId>");
			}
			Properties props = new Properties();
			props.load(AddWork.class.getClassLoader().getResourceAsStream("aws.properties"));

			String queueName = args[0];

			// Retrieve the message queue object (by name).
			QueueService qs = new QueueService(props.getProperty("aws.accessId"),
						props.getProperty("aws.secretKey"));
			MessageQueue msgQueue = qs.getOrCreateMessageQueue(queueName);

			// Try to retrieve (dequeue) the message, and then delete it.
			Message msg = null;
			while (true) {
				msg = msgQueue.receiveMessage();
				if (msg == null) {
					logger.info("nothing... retrying");
					try { Thread.sleep(1000); } catch (Exception ex) {}
					continue;
				}
				String text = msg.getMessageBody();
				logger.debug("msg : "+text);
				msgQueue.deleteMessage(msg);
				logger.info( "Deleted message id " + msg.getMessageId());
				count++;
			}
		} catch ( Exception ex ) {
			logger.error( "EXCEPTION", ex );
		}
		logger.debug("Deleted "+count+" messages");
	}
}
