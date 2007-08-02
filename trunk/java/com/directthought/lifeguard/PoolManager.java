
package com.directthought.lifeguard;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import com.directthought.lifeguard.util.QueueUtil;

public class PoolManager implements Runnable {
	private static Log logger = LogFactory.getLog(PoolManager.class);
	private static final boolean NO_LAUNCH = false;	// used for testing... don't really launch servers
	private static final int RECEIVE_COUNT = 20;
	private static final int IDLE_BUMP_INTERVAL = 120000;	// 2 minutes

	// configuration items
	private String awsAccessId;
	private String awsSecretKey;
	private String serverGroupName;
	private ServicePool config;
	private PoolMonitor monitor;

	// runtime data - stuff to save when saving state
	private List<Instance> instances;

	// transient data - not important to save
	private String usrData;
	private boolean keepRunning = true;

	/**
	 * This constructs a queue manager.
	 */
	public PoolManager() {
		instances = new ArrayList<Instance>();
	}

	public void setAccessId(String id) {
		awsAccessId = id;
	}

	public void setSecretKey(String key) {
		awsSecretKey = key;
	}

	public void setQueuePrefix(String prefix) {
		serverGroupName = prefix;
	}

	public void setPoolConfig(ServicePool config) {
		this.config = config;
	}

	public void setPoolMonitor(PoolMonitor monitor) {
		this.monitor = monitor;
	}

	public void run() {
		usrData = awsAccessId+" "+awsSecretKey+" "+serverGroupName+" "+config.getServiceName();
		// set pool monitor properties
		if (this.monitor != null) {
			this.monitor.setServiceName(config.getServiceName());
			this.monitor.setStatusQueue(config.getPoolStatusQueue());
			this.monitor.setWorkQueue(config.getServiceWorkQueue());
		}
		try {
			// fire up min servers first. They take a least 2 minutes to start up
			int min = config.getMinSize();
			if (min > 0) {
				launchInstances(min);
			}
			QueueService qs = new QueueService(awsAccessId, awsSecretKey);
			MessageQueue statusQueue = QueueUtil.getQueueOrElse(qs, serverGroupName+config.getPoolStatusQueue());
			MessageQueue workQueue = QueueUtil.getQueueOrElse(qs, serverGroupName+config.getServiceWorkQueue());

			long startBusyInterval = 0;	// used to track time pool has no idle capacity
			long startIdleInterval = 0;	// used to track time pool has spare capacity

			// now, loop forever, checking for busy status and checking work queue size
			logger.info("Starting PoolManager for service : "+config.getServiceName());
			while (keepRunning) {
				Message [] msgs = null;
				try {
					msgs = statusQueue.receiveMessages(RECEIVE_COUNT);
				} catch (SQSException ex) {
					logger.error("Error reading message, Retrying.", ex);
				}
				for (Message msg : msgs) {
					if (!keepRunning) break;	// fast exit
					if (msg != null) {	// process status message
						// parse it, then deal with it
						try {
							InstanceStatus status = JAXBuddy.deserializeXMLStream(InstanceStatus.class,
										new ByteArrayInputStream(msg.getMessageBody().getBytes()));
							// assume we have a change of state, so move instance between busy/idle
							String id = status.getInstanceId();
							//logger.debug("received instance status "+id+" is "+status.getState());
							int idx = instances.indexOf(new Instance(id));
							if (idx > -1) {
								Instance i = instances.get(idx);
								long interval =
										status.getLastInterval().getTimeInMillis(new Date(0));
								if (status.getState().equals("busy")) {
									i.lastIdleInterval = interval;
									if (this.monitor != null) {
										monitor.instanceBusy(id);
									}
								}
								else if (status.getState().equals("idle")) {
									i.lastBusyInterval = interval;
									if (this.monitor != null) {
										monitor.instanceIdle(id);
									}
								}
								else {
								}
								i.lastReportTime = System.currentTimeMillis();
								i.updateLoad();
							}
							else {
								logger.debug("ignoring message for instance not known");
							}
						} catch (JAXBException ex) {
							logger.error("Problem parsing instance status!", ex);
						}
						statusQueue.deleteMessage(msg);
						msg = null;
					}
					else {
						// if no messages, break out of status check loop, to main pool manage loop
						break;
					}
				}
				// for servers that haven't reported recently, bump idle interval...
				for (Instance i : instances) {
					// if more than a minute (arbitrarily) has gone by without a report,
					// increase the lastBusyInterval, and recalc the loadEstimate
					if (i.lastReportTime < (System.currentTimeMillis()-IDLE_BUMP_INTERVAL)) {
						i.lastIdleInterval += IDLE_BUMP_INTERVAL;
						i.lastReportTime = System.currentTimeMillis();
						i.updateLoad();
					}
				}

				// calculate pool load average
				int sum = 0;
				for (Instance i : instances) {
					sum += i.loadEstimate;
				}
				int denom = instances.size();
				int poolLoad = (denom==0)?0:(sum / denom);

				// now, see if were full busy, or somewhat idle
				if (poolLoad > 75) { // busy!
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
					int idleInterval = (startIdleInterval==0)?0:
								(int)(System.currentTimeMillis() - startIdleInterval) / 1000;
					int busyInterval = (startBusyInterval==0)?0:
								(int)(System.currentTimeMillis() - startBusyInterval) / 1000;
					int totalServers = instances.size();
					logger.debug("queue:"+queueDepth+
								" servers:"+totalServers+
								" load:"+poolLoad+
								" ii:"+idleInterval+" bi:"+busyInterval);
					if (idleInterval >= config.getRampDownDelay()) {	// idle interval has elapsed
						if (totalServers > config.getMinSize()) {
							// terminate as many servers (up to the interval)
							int numToKill = Math.min(config.getRampDownInterval(), instances.size());
							// ensure we don't kill too many servers (not below min)
							if ((totalServers-numToKill) < config.getMinSize()) {
								numToKill -= config.getMinSize() - (totalServers-numToKill);
							}
							// if there are still messages in work queue, leave an idle server
							// (this helps prevent cyclic launching and terminating of servers)
							if (queueDepth > 1 && (numToKill == instances.size())) {
								numToKill --;
							}

							if (numToKill > 0) {
								// grab the instances with the lowest load estimate
								Collections.sort(instances);
								Instance [] ids = new Instance[numToKill];
								for (int i=0; i<numToKill; i++) {
									ids[i] = instances.get(i);
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
							numToRun = numToRun * (int)((queueDepth / (float)(sizeFactor<1?1:sizeFactor))+1);
							if ((totalServers+numToRun) > config.getMaxSize()) {
								numToRun -= (totalServers+numToRun) - config.getMaxSize();
							}
							if (numToRun > 0) {
								launchInstances(numToRun);
							}
						}
					}
					// this test will get servers started if there is work and zero servers.
					if (totalServers == 0 && queueDepth > 0 && config.getMaxSize() > 0) {
						launchInstances(config.getRampUpInterval());
						startIdleInterval = 0;	// reset
						startBusyInterval = 0;	// reset
					}
				} catch (SQSException ex) {
					logger.error("Error getting queue depth, Retrying.", ex);
				}
//				logger.info("loop bottom");
				try { Thread.sleep(4000); } catch (InterruptedException iex) { }
			}
			// when loop exits, shut down all instances
			logger.info("Shutting down PoolManager for service : "+config.getServiceName());
			terminateInstances(instances.toArray(new Instance [] {}));
			instances.clear();
		} catch (Throwable t) {
			logger.error("something went horribly wrong in the pool manager main loop!", t);
		}
	}

	public void shutdown() {
		keepRunning = false;
	}

	// Launches server(s) with user data of "accessId secretKey serverGroupName serviceName"
	private void launchInstances(int numToLaunch) {
		logger.debug("Starting "+numToLaunch+" server(s)");
		try {
			if (NO_LAUNCH) {
				for (int i=0; i<numToLaunch; i++) {
					String fakeId = "i-"+(""+System.currentTimeMillis()+i).substring(6);
					logger.debug("not launching, using fake instance id : "+fakeId);
					instances.add(new Instance(fakeId));
					if (this.monitor != null) {
						monitor.instanceStarted(fakeId);
					}
				}
			}
			else {
				Jec2 ec2 = new Jec2(awsAccessId, awsSecretKey);
				ReservationDescription result = ec2.runInstances(config.getServiceAMI(),
															1, numToLaunch, null,
															usrData, serverGroupName+"-keypair");
				List<ReservationDescription.Instance> servers = result.getInstances();
				if (servers.size() < numToLaunch) {
					logger.warn("Failed to lanuch desired number of servers. ("
									+servers.size()+" instead of "+numToLaunch+")");
				}
				for (ReservationDescription.Instance s : servers) {
					instances.add(new Instance(s.getInstanceId()));
					if (this.monitor != null) {
						monitor.instanceStarted(s.getInstanceId());
					}
				}
			}
		} catch (EC2Exception ex) {
			logger.warn("Failed to launch instance(s). Will retry");
		}
	}

	private void terminateInstances(Instance [] instances) {
		logger.debug("Stopping server(s)");
		try {
			if (!NO_LAUNCH) {
				String [] ids = new String[instances.length];
				int x=0;
				for (Instance i : instances) {
					ids[x++] = i.id;
				}
				Jec2 ec2 = new Jec2(awsAccessId, awsSecretKey);
				ec2.terminateInstances(ids);
			}
			for (Instance i : instances) {
				this.instances.remove(i);
				if (this.monitor != null) {
					monitor.instanceTerminated(i.id);
				}
			}
		} catch (EC2Exception ex) {
			logger.warn("Failed to terminate instance. Will retry");
		}
	}

	private class Instance implements Comparable {
		String id;
		int loadEstimate;
		long lastIdleInterval;	// last reported interval of idle-ness
		long lastBusyInterval;	// last reported interval of busy-ness
		long lastReportTime;

		Instance(String id) {
			this.id = id;
			loadEstimate = 0;
			lastIdleInterval = 0;
			lastBusyInterval = 0;
			lastReportTime = System.currentTimeMillis();
		}


		void updateLoad() {
			loadEstimate = (int)(lastBusyInterval /
							(float)(lastIdleInterval+lastBusyInterval) * 100);
		}

		public boolean equals(Object o) {
			return (id.equals(((Instance)o).id));
		}

		public int compareTo(Object i) {
			return (loadEstimate - ((Instance)i).loadEstimate);
		}
	}
}
