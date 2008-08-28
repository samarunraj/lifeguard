
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.sqs.QueueService;
import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.Message;

/**
 * This sample application creates a queue with the specified name (if the queue doesn't
 * already exist), and then sends (enqueues) a message to the queue.
 */
public class AddWork {
    private static Log logger = LogFactory.getLog(AddWork.class);

	public static void main( String[] args ) {
		try {
			if (args.length < 1) {
				logger.error("usage: AddWork <workqueue>");
				System.exit(-1);
			}

			Properties props = new Properties();
			props.load(AddWork.class.getClassLoader().getResourceAsStream("aws.properties"));

			// Create the message queue object
			QueueService qs = new QueueService(props.getProperty("aws.accessId"),
									props.getProperty("aws.secretKey"));
			qs.setProxyValues("trivia.wrc.xerox.com", 8080);
			MessageQueue msgQueue = qs.getOrCreateMessageQueue(args[0].trim());

			String msg = "<WorkRequest xmlns=\"http://lifeguard.directthought.com/doc/2007-11-20/\">"+
					"<Project>TestProj</Project>"+
					"<Batch>1001</Batch>"+
					"<ServiceName>ingestor</ServiceName>"+
					"<InputBucket>com.xerox.dak.adfdata</InputBucket>"+
					"<OutputBucket>com.xerox.dak.adfdata</OutputBucket>"+
					"<Input><Key>job-12345</Key><Type>application/jdf</Type><Location>S3</Location></Input>"+
					"<OutputKey type=\"application/jdf\">id-12267_cps-25.jdf</OutputKey>"+
					"</WorkRequest>";
			String msgId = msgQueue.sendMessage(msg);
			logger.info( "Sent message with id " + msgId );
		} catch ( Exception ex ) {
			logger.error( "EXCEPTION", ex );
		}
	}
}
