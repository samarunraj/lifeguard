
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import ch.inventec.Base64Coder;

import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.Message;
import com.xerox.amazonws.sqs.SQSException;
import com.xerox.amazonws.sqs.SQSUtils;

import com.directthought.lifeguard.PoolMonitor;

/**
 * 
 */
public class SimulateServers implements PoolMonitor {
    private static Log logger = LogFactory.getLog(SimulateServers.class);

	// properties
	private String awsAccessId;
	private String awsSecretKey;
	private String queuePrefix;
	private String serviceName;
	private String statusQueueName;
	private String workQueueName;

	//
	private HashMap<String, SimThread> instances;

	public SimulateServers() {
		instances = new HashMap<String, SimThread>();
	}

	public void setAccessId(String id) {
		awsAccessId = id;
	}

	public void setSecretKey(String key) {
		awsSecretKey = key;
	}

	public void setQueuePrefix(String prefix) {
		queuePrefix = prefix;
	}

	public void setServiceName(String name) {
		serviceName = name;
	}

	public void setStatusQueue(String name) {
		statusQueueName = name;
	}

	public void setWorkQueue(String name) {
		workQueueName = name;
	}

	public void instanceStarted(String id) {
		System.err.println(">>>>>>>>>>> instance started : "+id);
		instances.put(id, new SimThread(id));
	}

	public void instanceTerminated(String id) {
		System.err.println(">>>>>>>>>>> instance terminated : "+id);
		instances.remove(id).shutdown();
	}

	public void instanceBusy(String id) {
		System.err.println(">>>>>>>>>>> instance busy : "+id);
	}

	public void instanceIdle(String id) {
		System.err.println(">>>>>>>>>>> instance idle : "+id);
	}

	private class SimThread implements Runnable {
		private String id;
		private Thread t;
		private MessageQueue statusQueue = null;
		private MessageQueue workQueue = null;
		private long lastSend;

		public SimThread(String id) {
			this.id = id;
			lastSend = System.currentTimeMillis();
			t = new Thread(this);
			t.start();
		}

		public void run() {
//			logger.debug("+++++++++++++++++ running simulator for instance : "+id);
			while (statusQueue == null) {
				try {
					statusQueue = SQSUtils.connectToQueue(queuePrefix+statusQueueName, awsAccessId, awsSecretKey);
//			logger.debug("+++++++++++++++++ status queue name : "+statusQueueName);
				} catch (SQSException ex) {
					logger.warn("couldn't connect to queue ("+queuePrefix+statusQueueName+"), retrying");
					try { Thread.sleep(1000); } catch (InterruptedException e) {}
				}
			}
//			logger.debug("+++++++++++++++++ connected to status queue for instance : "+id);
			while (workQueue == null) {
				try {
					workQueue = SQSUtils.connectToQueue(queuePrefix+workQueueName, awsAccessId, awsSecretKey);
				} catch (SQSException ex) {
					logger.warn("couldn't connect to queue ("+queuePrefix+workQueueName+"), retrying");
					try { Thread.sleep(1000); } catch (InterruptedException e) {}
				}
			}
//			logger.debug("+++++++++++++++++ connected to work queue for instance : "+id);
			sendStatus("busy");
//			logger.debug("+++++++++++++++++ sent busy for instance : "+id);
			while (!t.isInterrupted()) {
				try { Thread.sleep(8000+(int)(Math.random()*20000)); } catch (InterruptedException e) {}
				if (!t.isInterrupted()) {
					sendStatus("idle");
//					logger.debug("+++++++++++++++++ sent idle for instance : "+id);
					int numMsgs = 0;
					do { // keep sleeping randomly until there are messages in the work queue
						try { Thread.sleep(1000+(int)(Math.random()*1000)); } catch (InterruptedException e) {}
						while (true) {
							try {
								numMsgs = workQueue.getApproximateNumberOfMessages();
								break;
							} catch (SQSException ex) {
								logger.warn("couldn't get num msgs. retrying");
								try { Thread.sleep(1000); } catch (InterruptedException e) {}
							}
						}
					} while (numMsgs == 0);
					if (!t.isInterrupted()) {
						sendStatus("busy");
					}
				}
			}
		}

		public void shutdown() {
			t.interrupt();
		}

		private void sendStatus(String state) {
			while (true) {
				try {
					long newTime = System.currentTimeMillis();
					int duration = (int)((newTime - lastSend) / 1000);	// duration in seconds
					String msg = "<InstanceStatus xmlns=\"http://lifeguard.dotech.com/doc/2007-06-12/\"><InstanceId>"+
						id+"</InstanceId><State>"+state+"</State><LastInterval>PT"
						+duration+"S</LastInterval><Timestamp></Timestamp></InstanceStatus>";
					String msgId = statusQueue.sendMessage( Base64Coder.encodeString(msg) );
					lastSend = newTime;
					break;
				} catch (SQSException ex) {
					logger.warn("couldn't send message to queue ("+statusQueue+"), retrying");
					try { Thread.sleep(1000); } catch (InterruptedException e) {}
				}
			}
		}
	}
}
