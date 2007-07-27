
package com.directthought.lifeguard;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import com.directthought.lifeguard.jaxb.ObjectFactory;
import com.directthought.lifeguard.jaxb.Service;
import com.directthought.lifeguard.jaxb.Workflow;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.WorkStatus;

/**
 * This class implements the ingestion process. Classes that extend this need to configure
 * the ingestion based on how the files are captured (zip, GUI, etc...)
 */
public abstract class IngestorBase {
	private static Log logger = LogFactory.getLog(IngestorBase.class);

	private String awsAccessId;
	private String awsSecretKey;
	private String queuePrefix;
	private String project;
	private String batch;
	private String inputBucket;
	private String outputBucket;
	private String statusQueueName;
	private Workflow workflow;

	/**
	 *
	 */
	protected IngestorBase(String awsAccessId, String awsSecretKey, String queuePrefix,
							String project, String batch,
							String inputBucket, String outputBucket,
							String statusQueueName, Workflow workflow) {
		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
		this.queuePrefix = queuePrefix;
		this.project = project;
		this.batch = batch;
		this.inputBucket = inputBucket;
		this.outputBucket = outputBucket;
		this.statusQueueName = statusQueueName;
		this.workflow = workflow;
	}

	public void ingest(List<File> files) {
		ObjectFactory of = new ObjectFactory();
		Service first = workflow.getServices().get(0);

		// connect to queues
		QueueService qs = new QueueService(awsAccessId, awsSecretKey);
		MessageQueue statusQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+statusQueueName);
		MessageQueue workQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+first.getWorkQueue());

		try {
			for (File file : files) {
				long startTime = System.currentTimeMillis();
				// put file in S3 input bucket
				String s3Key = file.getPath();
				RestS3Service s3 = new RestS3Service(new AWSCredentials(awsAccessId, awsSecretKey));
				S3Object obj = new S3Object(new S3Bucket(inputBucket), file);
				obj = s3.putObject(inputBucket, obj);
				// send work request message
				WorkRequest wr = of.createWorkRequest();
				wr.setProject(project);
				wr.setBatch(batch);
				wr.setServiceName("ingestor");
				wr.setInputBucket(inputBucket);
				wr.setOutputBucket(outputBucket);
				FileRef ref = of.createFileRef();
				ref.setKey(s3Key);
				ref.setType("image/tiff");
				ref.setLocation("");
				wr.setInput(ref);
				long endTime = System.currentTimeMillis();
				String message = JAXBuddy.serializeXMLString(WorkRequest.class, wr);
				workQueue.sendMessage(message);
				// send work status message
				WorkStatus ws = MessageHelper.createWorkStatus(wr, file.getName(), startTime, endTime, "localhost");
				message = JAXBuddy.serializeXMLString(WorkStatus.class, ws);
				statusQueue.sendMessage(message);
			}
		} catch (SQSException ex) {
		} catch (IOException ex) {
		} catch (Exception ex) {
			logger.error(ex);
		}
	}
}
