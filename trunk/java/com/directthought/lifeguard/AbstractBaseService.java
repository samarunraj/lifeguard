
package com.directthought.lifeguard;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;
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

import com.directthought.lifeguard.jaxb.InstanceStatus;
import com.directthought.lifeguard.jaxb.ServiceConfig;
import com.directthought.lifeguard.jaxb.Step;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.WorkStatus;
import com.directthought.lifeguard.util.MD5Util;
import com.directthought.lifeguard.util.QueueUtil;

public abstract class AbstractBaseService implements Runnable {
	private static Log logger = LogFactory.getLog(AbstractBaseService.class);
	private ServiceConfig config;
	private String accessId;
	private String secretKey;
	private String queuePrefix;
	private long lastTime;

	public AbstractBaseService(ServiceConfig config, String accessId, String secretKey, String queuePrefix) {
		this.config = config;
		this.accessId = accessId;
		this.secretKey = secretKey;
		this.queuePrefix = queuePrefix;
	}

	public abstract List<MetaFile> executeService(File inputFile, WorkRequest request);

	public class MetaFile {
		public File file;
		public String mimeType;
		protected String key;

		public MetaFile(File file, String mimeType) {
			this.file = file;
			this.mimeType = mimeType;
		}
	}

	public String getServiceName() {
		return config.getServiceName();
	}

	public void run() {
		try {
			// connect to queues
			QueueService qs = new QueueService(accessId, secretKey);
			MessageQueue poolStatusQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+config.getPoolStatusQueue());
			MessageQueue workQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+config.getServiceWorkQueue());
			MessageQueue workStatusQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+config.getWorkStatusQueue());
			lastTime = System.currentTimeMillis();

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
						try { Thread.sleep(2000); } catch (InterruptedException ex) {}
						continue;
					}
					sendPoolStatus(poolStatusQueue, true);
					// parse work
					try {
						long startTime = System.currentTimeMillis();
						WorkRequest request = JAXBuddy.deserializeXMLStream(WorkRequest.class,
										new ByteArrayInputStream(msg.getMessageBody().getBytes()));
						// pull file from S3
						RestS3Service s3 = new RestS3Service(new AWSCredentials(accessId, secretKey));
						S3Bucket inBucket = new S3Bucket(request.getInputBucket());
						S3Object obj = s3.getObject(inBucket, request.getInput().getKey());
						InputStream iStr = obj.getDataInputStream();
						// should convert from mime-type to extension
						File inputFile = File.createTempFile("lg-", ".dat", new File("."));
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
						// call executeService()
						logger.debug("About to run service");
						List<MetaFile> results = executeService(inputFile, request);
						inputFile.delete();
						logger.debug("service produced "+results.size()+" results");
						// send results to S3
						for (MetaFile file : results) {
							file.key = MD5Util.md5Sum(new FileInputStream(file.file));
							S3Bucket outBucket = new S3Bucket(request.getOutputBucket());
							obj = new S3Object(outBucket, file.key);
							obj.setDataInputFile(file.file);
							obj.setContentLength(file.file.length());
							obj = s3.putObject(outBucket, obj);
							obj.closeDataInputStream();
						}
						// after all transferred, delete them locally
						for (MetaFile file : results) {
							file.file.delete();
						}
						long endTime = System.currentTimeMillis();
						// create status
						WorkStatus ws = MessageHelper.createWorkStatus(request, inputFile.getName(), startTime, endTime, "localhost");
						// send next work request
						Step next = request.getNextStep();
						if (next != null) {
							MessageQueue nextQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+next.getWorkQueue());
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
						QueueUtil.sendMessageForSure(workStatusQueue, message);
					// here's where we catch stuff that will be fatal for processing the message
					} catch (JAXBException ex) {
						logger.error("Problem parsing work request!", ex);
					}
					workQueue.deleteMessage(msg);
					sendPoolStatus(poolStatusQueue, false);
				// here's where we catch stuff that will cause us to re-try this later (keep it in Q)
				} catch (S3ServiceException ex) {
					logger.error("Problem with S3!", ex);
				}
			}
		} catch (Throwable t) {
			logger.error("Something unexpected happened in the "+getServiceName()+" service", t);
		}
	}

	private void sendPoolStatus(MessageQueue queue, boolean busy) {
		try {
			long interval = System.currentTimeMillis() - lastTime;
			InstanceStatus status = MessageHelper.createInstanceStatus("id", busy, interval);
			String message = JAXBuddy.serializeXMLString(InstanceStatus.class, status);
			QueueUtil.sendMessageForSure(queue, message);
			lastTime = System.currentTimeMillis();
		} catch (JAXBException ex) {
			logger.error("Problem serializing instance status!?", ex);
		} catch (IOException ex) {
			logger.error("Problem serializing instance status!?", ex);
		}
	}
}
