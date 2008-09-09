
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
import com.xerox.amazonws.ec2.InstanceType;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.LaunchConfiguration;
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

	// configuration items
	protected String awsAccessId;
	protected String awsSecretKey;
	protected String queuePrefix;
	protected ServicePool config;
	protected PoolMonitor monitor;
	protected int receiveCount = 20;
	protected int idleBumpInterval = 120000;
	protected long laggardLimit = 600000;
	protected int minLifetimeInMins = 0;
	protected String keypairName = "unknown-keypair";
	protected boolean noLaunch = false;
	protected int secondsToSleep = 4;
	protected String proxyHost;
	protected int proxyPort;

	// runtime data - stuff to save when saving state
	protected List<Instance> instances;

	// transient data - not important to save
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
		queuePrefix = prefix;
	}

	public void setPoolConfig(ServicePool config) {
		this.config = config;
	}

	public void setPoolMonitor(PoolMonitor monitor) {
		this.monitor = monitor;
	}

	public void setReceiveCount(int receiveCount) {
		this.receiveCount = receiveCount;
	}

	public void setIdleBumpInterval(int idleBumpInterval) {
		this.idleBumpInterval = idleBumpInterval;
	}

	public void setLaggardLimit(long laggardLimit) {
		this.laggardLimit = laggardLimit;
	}

	public void setMinimumLifetimeInMinutes(int minLifetimeInMins) {
		this.minLifetimeInMins = minLifetimeInMins;
	}

	public void setNoLaunch(boolean noLaunch) {
		this.noLaunch = noLaunch;
	}

	public void setKeypairName(String keypairName) {
		this.keypairName = keypairName;
	}

	public void setSecondsToSleep(int secs) {
		this.secondsToSleep = secs;
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

	protected String getUserData() {
		return awsAccessId+" "+awsSecretKey+" "+queuePrefix+" "+config.getServiceName();
	}

	public void run() {
		// set pool monitor properties
		if (this.monitor != null) {
			this.monitor.setServiceName(config.getServiceName());
			this.monitor.setStatusQueue(config.getPoolStatusQueue());
			this.monitor.setWorkQueue(config.getServiceWorkQueue());
		}
		try {
			logger.debug("mark 1");
			// Find existing servers.
			if (config.isFindExistingServers()) {
				listInstances();
			}

			logger.debug("mark 2");
			// fire up min servers first. They take a least 2 minutes to start up
			int min = config.getMinSize();
			if (min > instances.size()) {
				launchInstances(min - instances.size());
			}
			logger.debug("mark 3 "+awsAccessId);
			logger.debug("mark 3 "+awsSecretKey);
			QueueService qs = new QueueService(awsAccessId, awsSecretKey);
			if (!proxyHost.trim().equals("")) {
				logger.debug("proxy being set");
				qs.setProxyValues(proxyHost, proxyPort);
			}
			logger.debug("mark 4 "+queuePrefix+config.getPoolStatusQueue());
			MessageQueue statusQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+config.getPoolStatusQueue());
			logger.debug("mark 5");
			MessageQueue workQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+config.getServiceWorkQueue());

			logger.debug("mark 6");
			long startBusyInterval = 0;	// used to track time pool has no idle capacity
			long startIdleInterval = 0;	// used to track time pool has spare capacity

			// now, loop forever, checking for busy status and checking work queue size
			logger.debug("Starting PoolManager for service : "+config.getServiceName());
			while (keepRunning) {
				Message [] msgs = new Message[0];
				try {
					msgs = statusQueue.receiveMessages(receiveCount);
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
							String id = status.getInstanceId();
							//logger.debug("received instance status "+id+" is "+status.getState());
							int idx = instances.indexOf(new Instance(id));
							if (idx > -1) {
								Instance i = instances.get(idx);
								i.lastReportTime = status.getTimestamp().toGregorianCalendar().getTimeInMillis();
								i.loadEstimate = status.getDutyCycle().intValue();
								if (status.getState().equals("busy")) {
									if (this.monitor != null) {
										monitor.instanceBusy(id, i.loadEstimate);
									}
								}
								else if (status.getState().equals("idle")) {
									if (this.monitor != null) {
										monitor.instanceIdle(id, i.loadEstimate);
									}
								}
								else {
								}
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
					logger.info("queue:"+queueDepth+
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
							if (queueDepth >= 1 && (numToKill == instances.size())) {
								numToKill --;
							}

							if (numToKill > 0) {
								// grab the instances with the lowest load estimate
								Collections.sort(instances);
								Instance [] ids = new Instance[numToKill];
								for (int i=0; i<numToKill; i++) {
									ids[i] = instances.get(i);
								}
								terminateInstances(ids, false);
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
				// for servers that haven't reported recently, see if they are deliquent enough to
				// terminate and replace
				long unresponsive = laggardLimit / 3;
				for (Instance i : instances) {
					// report possible failure if 1/3 of laggardLimit has gone by
					if (i.lastReportTime < (System.currentTimeMillis()-unresponsive)) {
						if (this.monitor != null) {
							monitor.instanceUnresponsive(i.id);
						}
					}
					// if more than N minutes have gone by without a report, do the needful
					if (i.lastReportTime < (System.currentTimeMillis()-laggardLimit)) {
						logger.error("Instance "+i.id+" is being replaced");
						terminateInstances(new Instance [] {i}, true);
						launchInstances(1);
					}
				}

//				logger.info("loop bottom");
				try { Thread.sleep(secondsToSleep*1000); } catch (InterruptedException iex) { }
			}
			// when loop exits, shut down all instances
			logger.info("Shutting down PoolManager for service : "+config.getServiceName());
			terminateInstances(instances.toArray(new Instance [] {}), true);
			instances.clear();
		} catch (Throwable t) {
			logger.error("something went horribly wrong in the pool manager main loop!", t);
		}
	}

	public void shutdown() {
		keepRunning = false;
	}

	// Finds any EC2 instances based on the appropriate AMI that are already running
	private void listInstances() throws EC2Exception {
		Jec2 ec2 = new Jec2(awsAccessId, awsSecretKey);
		if (!proxyHost.trim().equals("")) {
			ec2.setProxyValues(proxyHost, proxyPort);
		}
		List<String> params = new ArrayList<String>();
		List<ReservationDescription> reservations = ec2.describeInstances(params);

		for (ReservationDescription rd : reservations) {
			for (ReservationDescription.Instance i : rd.getInstances()) {
				if (i != null && i.getImageId().equals(config.getServiceAMI())
				    && (i.getState().equals("pending") || i.getState().equals("running"))) {
					logger.info("Found " + i.getState() + " instance: " + i.getInstanceId());
					instances.add(new Instance(i.getInstanceId(), i.getPrivateDnsName()));
					if (this.monitor != null) {
						monitor.instanceStarted(i.getInstanceId());
					}
				}
			}
		}
	}

	// Launches server(s) with user data of "accessId secretKey queuePrefix serviceName"
	private void launchInstances(int numToLaunch) {
		logger.debug("Starting "+numToLaunch+" server(s)");
		try {
			if (noLaunch) {
				for (int i=0; i<numToLaunch; i++) {
					String counter = "0000000" + (instances.size()+1);
					counter = counter.substring(counter.length() - 8);
					String fakeId = "i-" + counter;
					logger.debug("Not launching, using fake instance id : "+fakeId);
					logger.debug("User Data for fake service: "+getUserData());
					instances.add(new Instance(fakeId));
					if (this.monitor != null) {
						monitor.instanceStarted(fakeId);
					}
				}
			}
			else {
				Jec2 ec2 = new Jec2(awsAccessId, awsSecretKey);
				if (!proxyHost.trim().equals("")) {
					ec2.setProxyValues(proxyHost, proxyPort);
				}
				LaunchConfiguration lc =
							new LaunchConfiguration(config.getServiceAMI(), 1, numToLaunch);
				lc.setUserData(getUserData().getBytes());
				lc.setKeyName(keypairName);
				String type = config.getInstanceType();
				if (type!=null && !type.trim().equals("")) {
					lc.setInstanceType(InstanceType.getTypeFromString(type));
				}
				String kernel = config.getKernel();
				if (kernel!=null && !kernel.trim().equals("")) {
					lc.setKernelId(kernel);
				}
				ReservationDescription result = ec2.runInstances(lc);
				List<ReservationDescription.Instance> servers = result.getInstances();
				if (servers.size() < numToLaunch) {
					logger.warn("Failed to lanuch desired number of servers. ("
									+servers.size()+" instead of "+numToLaunch+")");
				}
				for (ReservationDescription.Instance s : servers) {
					instances.add(new Instance(s.getInstanceId(), s.getPrivateDnsName()));
					if (this.monitor != null) {
						monitor.instanceStarted(s.getInstanceId());
					}
				}
			}
		} catch (EC2Exception ex) {
			logger.warn("Failed to launch instance(s). Will retry");
		}
	}

	private void terminateInstances(Instance [] instances, boolean force) {
		logger.debug("Stopping server(s)");
		try {
			if (!noLaunch) {
				String [] ids = new String[instances.length];
				int x=0;
				for (Instance i : instances) {
					// Don't stop instances before minLifetimeInMins
					if (!i.isMinLifetimeElapsed() && !force) {
						logger.debug("Keeping instance "+i.id+
							" alive until it has lived for "+
							minLifetimeInMins+" mins");
						continue;
					}
					ids[x++] = i.id;
				}
				if (x == 0) return;
				Jec2 ec2 = new Jec2(awsAccessId, awsSecretKey);
				if (!proxyHost.trim().equals("")) {
					ec2.setProxyValues(proxyHost, proxyPort);
				}
				ec2.terminateInstances(ids);
			}
			for (Instance i : instances) {
				// Don't stop instances before minLifetimeInMins
				if (!i.isMinLifetimeElapsed() && !force) {
					logger.debug("Keeping instance "+i.id+
						" alive until it has lived for "+
						minLifetimeInMins+" mins");
					continue;
				}
				this.instances.remove(i);
				if (this.monitor != null) {
					monitor.instanceTerminated(i.id);
				}
			}
		} catch (EC2Exception ex) {
			logger.warn("Failed to terminate instance. Will retry", ex);
		}
	}

	private class Instance implements Comparable {
		String id;
		String hostName;
		int loadEstimate;
		long lastReportTime;
		long startupTime;		// the time this instances was first started

		Instance(String id) {
			this(id, null);
		}

		Instance(String id, String hostName) {
			this.id = id;
			this.hostName = hostName;
			loadEstimate = 0;
			lastReportTime = System.currentTimeMillis();
			startupTime = System.currentTimeMillis();
		}


		public boolean equals(Object o) {
			return (id.equals(((Instance)o).id));
		}

		public int compareTo(Object i) {
			Instance otherInstance = (Instance)i;

			// Compare the elapsed lifetime status. If the status differs, instances
			// that have lived beyond the minimum lifetime will be sorted earlier.
			if (isMinLifetimeElapsed() != otherInstance.isMinLifetimeElapsed()) {
				if (isMinLifetimeElapsed()) {
					// This instance has lived long enough, the other hasn't
					return -1;
				} else {
					// The other instance has lived long enough, this one hasn't
					return 1;
				}
			}

			return (loadEstimate - otherInstance.loadEstimate);
		}

		public boolean isMinLifetimeElapsed() {
			long runTimeSecs = (System.currentTimeMillis() - startupTime) / 1000;
			return (runTimeSecs > (minLifetimeInMins * 60)); 
		}
	}
}
