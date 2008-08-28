
package com.directthought.lifeguard;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import com.xerox.amazonws.common.JAXBuddy;
import com.xerox.amazonws.sqs.Message;
import com.xerox.amazonws.sqs.MessageQueue;
import com.xerox.amazonws.sqs.QueueService;
import com.xerox.amazonws.sqs.SQSException;

import com.directthought.lifeguard.jaxb.FileRef;
import com.directthought.lifeguard.jaxb.InstanceStatus;
import com.directthought.lifeguard.jaxb.ServiceConfig;
import com.directthought.lifeguard.jaxb.Step;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.WorkRequest.OutputKey;
import com.directthought.lifeguard.jaxb.WorkStatus;
import com.directthought.lifeguard.util.MD5Util;
import com.directthought.lifeguard.util.QueueUtil;

public abstract class AbstractBaseService implements Runnable {
	private static Log logger = LogFactory.getLog(AbstractBaseService.class);
	private static final long MIN_STATUS_INTERVAL = 60000;
	private ServiceConfig config;
	private String accessId;
	private String secretKey;
	private String queuePrefix;
	private String instanceId = "unknown";
	private long lastTime;
	protected int secondsToSleep = 4;

	public AbstractBaseService(ServiceConfig config, String accessId, String secretKey, String queuePrefix) {
		this.config = config;
		this.accessId = accessId;
		this.secretKey = secretKey;
		this.queuePrefix = queuePrefix;
		String id = config.getInstanceIdOverride();
		if (id != null && !id.equals("")) {
			instanceId = id;
		}
		else {
			int iter = 0;
			while (true) {
				try {
					URL url = new URL("http://169.254.169.254/1.0/meta-data/instance-id");
					instanceId = new BufferedReader(new InputStreamReader(url.openStream())).readLine();
					break;
				} catch (IOException ex) {
					if (iter == 5) {
						logger.debug("Problem getting instance data, retries exhausted...");
						break;
					}
					else {
						logger.debug("Problem getting instance data, retrying...");
						try { Thread.sleep((iter+1)*1000); } catch (InterruptedException iex) {}
					}
				}
				iter++;
			}
		}
	}

	public abstract List<MetaFile> executeService(File inputFile, WorkRequest request) throws ServiceException;

	public String getServiceName() {
		return config.getServiceName();
	}

	public void setSecondsToSleep(int secs) {
		this.secondsToSleep = secs;
	}

	public void run() {
		try {
			// connect to queues
			QueueService qs = new QueueService(accessId, secretKey);
			MessageQueue poolStatusQueue =
						QueueUtil.getQueueOrElse(qs, queuePrefix+config.getPoolStatusQueue());
			MessageQueue workQueue =
						QueueUtil.getQueueOrElse(qs, queuePrefix+config.getServiceWorkQueue());
			MessageQueue workStatusQueue =
						QueueUtil.getQueueOrElse(qs, queuePrefix+config.getWorkStatusQueue());

			new StatusSender(poolStatusQueue).start();

			File tmpDir = new File(".");
			String dirName = config.getTempDirectory();
			if (dirName != null && !dirName.equals("")) {
				tmpDir = new File(dirName);
				logger.debug("Using temp directory : "+dirName);
				if (!tmpDir.exists()) {
					tmpDir.mkdirs();
				}
			}
			lastTime = System.currentTimeMillis();

			MessageWatcher msgWatcher = null;
			while (true) {
				try {
					// read work queue
					Message msg = null;
					try {
						msg = workQueue.receiveMessage();
					} catch (SQSException ex) {
						logger.error("Error reading message, Retrying.", ex);
					}
					if (msg == null) {
						logger.debug("no message, sleeping...");
						try { Thread.sleep(secondsToSleep*1000); } catch (InterruptedException ex) {}
						continue;
					}
					else {
						// start message watcher which bumps visibility timeuot while we process
						if (msgWatcher != null) {	// just in case...
							msgWatcher.interrupt();
						}
						msgWatcher = new MessageWatcher(workQueue, msg);
						msgWatcher.start();
					}
					setPoolStatus(true);
					// parse work
					WorkRequest request = null;
					long startTime = System.currentTimeMillis();
					File inputFile = null;
					try {
						request = JAXBuddy.deserializeXMLStream(WorkRequest.class,
										new ByteArrayInputStream(msg.getMessageBody().getBytes()));
						// change service name to that of the current service.
						request.setServiceName(getServiceName());
						// pull file from S3
						RestS3Service s3 = new RestS3Service(new AWSCredentials(accessId, secretKey));
						// check for input required
						String bucketName = request.getInputBucket();
						FileRef inFile = request.getInput();
						if ((bucketName != null && !bucketName.equals("")) && (inFile != null)) {
							S3Bucket inBucket = new S3Bucket(bucketName);
							S3Object obj = s3.getObject(inBucket, inFile.getKey());
							InputStream iStr = obj.getDataInputStream();
							// should convert from mime-type to extension
							inputFile = File.createTempFile("lg-", ".tmp", tmpDir);
							byte [] buf = new byte[64*1024];	// 64k i/o buffer
							FileOutputStream oStr = new FileOutputStream(inputFile);
							int count = iStr.read(buf);
							while (count != -1) {
								if (count > 0) {
									oStr.write(buf, 0, count);
								}
								count = iStr.read(buf);
							}
							oStr.close();
						}
						// call executeService()
						logger.debug("About to run service");
						List<MetaFile> results = executeService(inputFile, request);
						if (inputFile != null) {
							inputFile.delete();
						}
						logger.debug("service produced "+((results==null)?0:results.size())+" results");
						// see if we work request specified output keys
						List<OutputKey> keys = request.getOutputKeies(); // don't ask about the spelling.. jaxb did it
						if (keys != null && keys.size() > 0) {	// see if there is a key to match the mimetype of this file
							// see if there are multiples of any given type
							int [] totals = new int[keys.size()];
							Arrays.fill(totals, 0);
							for (MetaFile file : results) {
								for (int i=0; i<keys.size(); i++) {
									OutputKey key = keys.get(i);
									if (key.getType().equals(file.mimeType)) {
										totals[i]++;
									}
								}
							}
							// now set the output keys
							int [] counter = new int[keys.size()];
							Arrays.fill(totals, 0);
							for (MetaFile file : results) {
								for (int i=0; i<keys.size(); i++) {
									OutputKey key = keys.get(i);
									if (key.getType().equals(file.mimeType)) {
										if (totals[i] > 1) {
											String k = key.getValue();
											int idx = k.lastIndexOf('/')+1;
											file.key = k.substring(0, idx) +
														counter[i] + k.substring(idx);
										}
										else {
											file.key = key.getValue();
										}
									}
								}
							}
						}
						// send results to S3
						for (MetaFile file : results) {
							if (file.key == null || file.key.trim().equals("")) {
								file.key = MD5Util.md5Sum(new FileInputStream(file.file));
							}
							S3Bucket outBucket = new S3Bucket(request.getOutputBucket());
							S3Object obj = new S3Object(outBucket, file.key);
							obj.setDataInputFile(file.file);
							obj.setContentLength(file.file.length());
							obj.setContentType(file.mimeType);
							obj = s3.putObject(outBucket, obj);
							obj.closeDataInputStream();
						}
						// after all transferred, delete them locally
						for (MetaFile file : results) {
							file.file.delete();
						}
						long endTime = System.currentTimeMillis();
						// create status
						WorkStatus ws = MessageHelper.createServiceStatus(request, results,
																startTime, endTime, instanceId);
						// send next work request
						Step next = request.getNextStep();
						if (next != null) {
							MessageQueue nextQueue = QueueUtil.getQueueOrElse(qs,
																queuePrefix+next.getWorkQueue());
							String mimeType = next.getType();
							for (MetaFile file : results) {
								if (file.mimeType.equals(mimeType)) {
									request.getInput().setType(mimeType);
									request.getInput().setKey(file.key);
								}
							}
							request.setNextStep(next.getNextStep());
							String message = JAXBuddy.serializeXMLString(WorkRequest.class, request);
							QueueUtil.sendMessageForSure(nextQueue, message);
						}
						// send status
						String message = JAXBuddy.serializeXMLString(WorkStatus.class, ws);
						logger.debug("sending works status message : "+message);
						QueueUtil.sendMessageForSure(workStatusQueue, message);

					// here's where we catch stuff that will be fatal for processing the message
					} catch (JAXBException ex) {
						logger.error("Problem parsing work request!", ex);
					} catch (ServiceException se) {
						logger.error("Problem executing service!", se);
						long endTime = System.currentTimeMillis();
						WorkStatus ws = MessageHelper.createServiceStatus(request, null,
															startTime, endTime, instanceId);
						ws.setFailureMessage(se.getMessage());
						String message = JAXBuddy.serializeXMLString(WorkStatus.class, ws);
						QueueUtil.sendMessageForSure(workStatusQueue, message);
					} finally {
						if (msgWatcher != null) {
							msgWatcher.interrupt();
							msgWatcher = null;
						}
					}
					if (inputFile != null && inputFile.exists()) {
						inputFile.delete();
						inputFile = null;
					}
					workQueue.deleteMessage(msg);
					setPoolStatus(false);

				// here's where we catch stuff that will cause a re-try of this later (keep it in Q)
				} catch (SocketException ex) {
					logger.error("Problem with communication with AWS!", ex);
				} catch (SocketTimeoutException ex) {
					logger.error("Problem with communication with AWS!", ex);
				} catch (S3ServiceException ex) {
					logger.error("Problem with S3!", ex);
				}
			}
		} catch (Throwable t) {
			logger.error("Something unexpected happened in the "+getServiceName()+" service", t);
		}
	}

	private boolean busy;
	private long lastBusyInterval = 0;
	private long lastIdleInterval = 0;

	private void setPoolStatus(boolean busy) {
		long now = System.currentTimeMillis();
		this.busy = busy;
		if (busy) {
			lastIdleInterval = now - lastTime;
		}
		else {
			lastIdleInterval = now - lastTime;
		}
		lastTime = now;
	}

	class StatusSender extends Thread {
		MessageQueue queue;

		StatusSender(MessageQueue queue) {
			this.queue = queue;
		}

		public void run() {
			while (true) {
				try {
					sendPoolStatus(); 
					try { Thread.sleep(30000); } catch (InterruptedException ex) {}
				} catch (Exception ex) {
					logger.error("Problem while sending status, retrying", ex);
				}
			}
		}

		private void sendPoolStatus() {
			try {
				int dutyCycle = (int)((lastBusyInterval / (float)(lastIdleInterval + lastBusyInterval)) * 100);
				InstanceStatus status = MessageHelper.createInstanceStatus(instanceId, busy, dutyCycle);
				String message = JAXBuddy.serializeXMLString(InstanceStatus.class, status);
				QueueUtil.sendMessageForSure(queue, message);
			} catch (JAXBException ex) {
				logger.error("Problem serializing instance status!?", ex);
			} catch (IOException ex) {
				logger.error("Problem serializing instance status!?", ex);
			}
		}
	}

	class MessageWatcher extends Thread {
		private MessageQueue queue;
		private Message msg;

		public MessageWatcher(MessageQueue queue, Message msg) {
			this.queue = queue;
			this.msg = msg;
		}

		public void run() {
			while (!isInterrupted()) {
				// sleep for 25 seconds, then bump that timeout
				try { Thread.sleep(25000); } catch (InterruptedException ex) { interrupt(); }
				while (!isInterrupted()) {
					try {
						queue.setVisibilityTimeout(msg, 30);
						break;
					} catch (SQSException ex) {
						logger.warn("Error setting visibility timeout, Retrying.");
						try { Thread.sleep(1000); } catch (InterruptedException iex) {}
					}
				}
			}
		}
	}
}
