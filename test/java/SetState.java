
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import ch.inventec.Base64Coder;

import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.Message;
import com.xerox.amazonws.sqs.SQSUtils;

/**
 * This sample application creates a queue with the specified name (if the queue doesn't
 * already exist), and then sends (enqueues) a message to the queue.
 */
public class SetState {
    private static Log logger = LogFactory.getLog(SetState.class);

	public static void main( String[] args ) {
		try {
			if (args.length < 4) {
				logger.error("usage: SetState <statusqueue> <instanceId> <state> <duration>");
			}
			String statusQueue = args[0];
			String instanceId = args[1];
			String state = args[2];
			String duration = args[3];

			Properties props = new Properties();
			props.load(SetState.class.getClassLoader().getResourceAsStream("aws.properties"));

			// Create the message queue object
			MessageQueue msgQueue = SQSUtils.connectToQueue(statusQueue,
					props.getProperty("aws.accessId"), props.getProperty("aws.secretKey"));

			String msg = "<InstanceStatus xmlns=\"http://lifeguard.directthought.com/doc/2007-11-20/\"><InstanceId>"+instanceId+"</InstanceId><State>"+state+"</State><LastInterval>P"+duration+"M</LastInterval><Timestamp></Timestamp></InstanceStatus>";
			String msgId = msgQueue.sendMessage(msg);
			logger.info( "Sent message with id " + msgId );
		} catch ( Exception ex ) {
			logger.error( "EXCEPTION", ex );
		}
	}
}
