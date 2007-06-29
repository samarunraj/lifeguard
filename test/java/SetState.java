
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
//		final String AWSAccessKeyId = "[AWS Access Id]";
//		final String SecretAccessKey = "[AWS Secret Key]";
        final String AWSAccessKeyId = "0ZZXAZ980M9J5PPCFTR2";
        final String SecretAccessKey = "4sWhM1t3obEYOr2ZkqbcwaWozM+ayVmKfRm/1rjC";

		try {
			if (args.length < 2) {
				logger.error("usage: SetState <instanceId> <state>");
			}
			String instanceId = args[0];
			String state = args[1];

			// Create the message queue object
			MessageQueue msgQueue = SQSUtils.connectToQueue("poolStatusTest", AWSAccessKeyId, SecretAccessKey);

			String msg = "<InstanceStatus xmlns=\"http://lifeguard.dotech.com/doc/2007-06-12/\"><InstanceId>"+instanceId+"</InstanceId><State>"+state+"</State><Timestamp></Timestamp></InstanceStatus>";
			String msgId = msgQueue.sendMessage( Base64Coder.encodeString(msg) );
			logger.info( "Sent message with id " + msgId );
		} catch ( Exception ex ) {
			logger.error( "EXCEPTION", ex );
		}
	}
}
