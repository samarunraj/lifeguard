
package com.directthought.lifeguard;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import ch.inventec.Base64Coder;

import com.xerox.amazonws.common.JAXBuddy;
import com.xerox.amazonws.ec2.EC2Exception;
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
	private static boolean NO_LAUNCH = true;	// used for testing... don't really launch servers

	// configuration items
	private String awsAccessId;
	private String awsSecretKey;
	private String serverGroupName;
	private ServicePool config;

	// runtime data - stuff to save when saving state
	private List<String> busyInstances;
	private List<String> idleInstances;

	// transient data - not important to save
	private String usrData;
	private boolean keepRunning = true;

	/**
	 * This constructs a queue manager.
	 *
	 * @param awsAccessId
	 * @param awsSecretKey
	 * @param serverGroupName
	 */
	public PoolManager(String awsAccessId, String awsSecretKey, String serverGroupName, ServicePool config) {
		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
		this.serverGroupName = serverGroupName;
		this.config = config;
		busyInstances = new ArrayList<String>();
		idleInstances = new ArrayList<String>();
		usrData = Base64Coder.encodeString(awsAccessId+" "+awsSecretKey+" "
											+serverGroupName+" "+config.getServiceName());
	}

	public void run() {
		try {
			// fire up min servers first. They take a least 2 minutes to start up
			int min = config.getMinSize();
			if (min > 0) {
				launchInstances(min);
			}
			QueueService qs = new QueueService(awsAccessId, awsSecretKey);
			MessageQueue statusQueue = getQueueOrElse(qs, serverGroupName+config.getPoolStatusQueue());
			MessageQueue workQueue = getQueueOrElse(qs, serverGroupName+config.getServiceWorkQueue());

			long startBusyInterval = 0;	// used to track time pool has no idle capacity
			long startIdleInterval = 0;	// used to track time pool has spare capacity

			// now, loop forever, checking for busy status and checking work queue size
			logger.info("Starting PoolManager for service : "+config.getServiceName());
			while (keepRunning) {
				Message msg = null;
				try {
					msg = statusQueue.receiveMessage();
				} catch (SQSException ex) {
					logger.error("Error reading message, Retrying.", ex);
				}
				if (!keepRunning) break;	// fast exit
				if (msg != null) {	// process status message
					logger.debug("got instance status message");
					// parse it, then deal with it
					try {
						InstanceStatus status = JAXBuddy.deserializeXMLStream(InstanceStatus.class,
									new ByteArrayInputStream(Base64Coder.decodeString(
															msg.getMessageBody()).getBytes()));
						// assume we have a change of state, so move instance between busy/idle
						String id = status.getInstanceId();
						if (status.getState().equals("busy")) {
							if (idleInstances.remove(id)) {
								busyInstances.add(id);
							}
							// else, it wasn't something we were managing
						}
						else if (status.getState().equals("idle")) {
							if (busyInstances.remove(id)) {
								idleInstances.add(id);
							}
						}
						// else, wouldn't parse so ignore this case

					} catch (JAXBException ex) {
						logger.error("Problem parsing instance status!", ex);
					}
					statusQueue.deleteMessage(msg);
					msg = null;
				}
				// now, see if were full busy, or somewhat idle
				if (idleInstances.size() == 0) { // busy!
					if (startBusyInterval == 0) {
						startBusyInterval = System.currentTimeMillis();
					}
					startIdleInterval = 0;
				}
				else {
					if (startIdleInterval == 0) {
						startIdleInterval = System.currentTimeMillis();
					}
					startBusyInterval = 0;
				}
				try {
					int queueDepth = workQueue.getApproximateNumberOfMessages();
					if (!keepRunning) break;	// fast exit
					// now, based on busy/idle timers and queue depth, make a call on
					// whether to start or terminate servers
					// TODO
					int idleInterval = (startIdleInterval==0)?0:
								(int)(System.currentTimeMillis() - startIdleInterval) / 1000;
					int busyInterval = (startBusyInterval==0)?0:
								(int)(System.currentTimeMillis() - startBusyInterval) / 1000;
					int totalServers = idleInstances.size() + busyInstances.size();
					logger.debug("queue:"+queueDepth+
								" idle:"+idleInstances.size()+" busy:"+busyInstances.size()+
								" ii:"+idleInterval+" bi:"+busyInterval);
					if (idleInterval >= config.getRampDownDelay()) {	// idle interval has elapsed
						if (totalServers > config.getMinSize()) {
							// terminate as many servers (up to the interval)
							int numToKill = Math.min(config.getRampDownInterval(), idleInstances.size());
							// ensure we don't kill too many servers (not below min)
							if ((totalServers-numToKill) < config.getMinSize()) {
								numToKill -= config.getMinSize() - (totalServers-numToKill);
							}
							// if there are still messages in work queue, leave an idle server
							// (this helps prevent cyclic launching and terminating of servers)
							if (queueDepth > 1 && (numToKill == idleInstances.size())) {
								numToKill --;
							}

							if (numToKill > 0) {
								String [] ids = new String[numToKill];
								for (int i=0; i<numToKill; i++) {
									ids[i] = idleInstances.get(i);
								}
								terminateInstances(ids);
							}
							startIdleInterval = 0;	// reset
						}
					}
					if (busyInterval >= config.getRampUpDelay()) {	// busy interval has elapsed
						if (totalServers < config.getMaxSize()) {
							int numToRun = config.getRampUpInterval();
							int sizeFactor = config.getQueueSizeFactor();
							// use queueDepth to adjust the numToRun
							numToRun = numToRun * (int)(queueDepth / (sizeFactor<1?1:sizeFactor));
							if ((totalServers+numToRun) > config.getMaxSize()) {
								numToRun -= (totalServers+numToRun) - config.getMaxSize();
							}
							if (numToRun > 0) {
								launchInstances(numToRun);
							}
						}
					}
				} catch (SQSException ex) {
					logger.error("Error getting queue depth, Retrying.", ex);
				}
//				logger.info("loop bottom");
				try { Thread.sleep(4000); } catch (InterruptedException iex) { }
			}
			// when loop exits, shut down all instances
			logger.info("Shutting down PoolManager for service : "+config.getServiceName());
			idleInstances.addAll(busyInstances);
			busyInstances.clear();
			terminateInstances(idleInstances.toArray(new String [] {}));
		} catch (Throwable t) {
			logger.error("something went horribly wrong in the pool manager main loop!", t);
		}
	}

	public void shutdown() {
		keepRunning = false;
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

	// Launches server(s) with user data of "accessId secretKey serverGroupName serviceName"
	private void launchInstances(int numToLaunch) {
		logger.debug("Starting "+numToLaunch+" server(s)");
		try {
			if (NO_LAUNCH) {
				for (int i=0; i<numToLaunch; i++) {
					String fakeId = "i-"+(""+System.currentTimeMillis()+i).substring(6);
					logger.debug("not launching, using fake instance id : "+fakeId);
					idleInstances.add(fakeId);
				}
			}
			else {
				Jec2 ec2 = new Jec2(awsAccessId, awsSecretKey);
				ReservationDescription result = ec2.runInstances(config.getServiceAMI(),
															numToLaunch, numToLaunch, null,
															usrData, serverGroupName+"-keypair");
				List<ReservationDescription.Instance> servers = result.getInstances();
				if (servers.size() < numToLaunch) {
					logger.warn("Failed to lanuch desired number of servers. ("
									+servers.size()+" instead of "+numToLaunch+")");
				}
				for (ReservationDescription.Instance s : servers) {
					idleInstances.add(s.getInstanceId());
				}
			}
		} catch (EC2Exception ex) {
			logger.warn("Failed to launch instance(s). Will retry");
		}
	}

	private void terminateInstances(String [] instanceIds) {
		logger.debug("Stopping server(s)");
		try {
			if (!NO_LAUNCH) {
				Jec2 ec2 = new Jec2(awsAccessId, awsSecretKey);
				ec2.terminateInstances(instanceIds);
			}
			for (String id : instanceIds) {
				idleInstances.remove(id);
			}
		} catch (EC2Exception ex) {
			logger.warn("Failed to terminate instance. Will retry");
		}
	}
}
