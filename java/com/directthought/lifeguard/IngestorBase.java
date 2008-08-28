
package com.directthought.lifeguard;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.activation.MimetypesFileTypeMap;
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
import com.directthought.lifeguard.jaxb.ParamType;
import com.directthought.lifeguard.jaxb.Service;
import com.directthought.lifeguard.jaxb.Step;
import com.directthought.lifeguard.jaxb.Workflow;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.WorkRequest.OutputKey;
import com.directthought.lifeguard.jaxb.WorkStatus;
import com.directthought.lifeguard.util.MD5Util;
import com.directthought.lifeguard.util.QueueUtil;

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

	// proxy values to pass onto typica and jets3t
	private String proxyHost;
	private int proxyPort;

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

	public void setProxyValues(String proxyHost, int proxyPort) {
		this.proxyHost = proxyHost;
		this.proxyPort = proxyPort;
	}

	/**
	 * This method takes a list of properties submits work requests for each. This is designed
	 * for workflows that don't require an initial input file, rather some properties unique to
	 * each request define the inputs.
	 *
	 * @param properties the list of properties to use
	 */
	public void ingestProperties(List<Properties> properties) {
		ingest(null, properties, null);
	}

	/**
	 * This method takes a list of keys of objects in S3 and submits work requests for each.
	 *
	 * @param files the list of files to ingest
	 */
	public void ingestKeys(List<MetaFile> keys) {
		ingest(keys, null, null);
	}

	/**
	 * This method takes a list of keys of objects in S3 and submits work requests for each.
	 *
	 * @param files the list of files to ingest
	 * @param outputKeys a map of keys,mimeTypes that specify keys to use to store output
	 */
	public void ingestKeys(List<MetaFile> keys, Map<String, String> outputKeys) {
		ingest(keys, null, outputKeys);
	}

	/**
	 * This method takes a list of files, moves the files to S3 and submits work requests for each.
	 *
	 * @param files the list of files to ingest
	 */
	public void ingestFiles(List<File> files) {
		ArrayList<MetaFile> mFiles = new ArrayList<MetaFile>();
		for (File file : files) {
			String type = new MimetypesFileTypeMap().getContentType(file);
			mFiles.add(new MetaFile(file, type));
		}
		ingest(mFiles, null, null);
	}

	protected void ingest(List<MetaFile> files, List<Properties> properties, Map<String, String> outputKeys) {
		ObjectFactory of = new ObjectFactory();

		// connect to queues
		QueueService qs = new QueueService(awsAccessId, awsSecretKey);
		if (proxyHost != null && !proxyHost.equals("")) {
			qs.setProxyValues(proxyHost, proxyPort);
		}
		MessageQueue statusQueue = QueueUtil.getQueueOrElse(qs, queuePrefix+statusQueueName);
		MessageQueue workQueue = QueueUtil.getQueueOrElse(qs,
										queuePrefix+workflow.getServices().get(0).getWorkQueue());

		try {
			// build common parts of work request
			WorkRequest wr = of.createWorkRequest();
			wr.setProject(project);
			wr.setBatch(batch);
			wr.setServiceName("ingestor");
			wr.setInputBucket(inputBucket);
			wr.setOutputBucket(outputBucket);
			if (outputKeys != null) {
				List<OutputKey> tmp = wr.getOutputKeies();
				for (String key : outputKeys.keySet()) {
					OutputKey ok = of.createWorkRequestOutputKey();
					ok.setValue(key);
					ok.setType(outputKeys.get(key));
					tmp.add(ok);
				}
			}
			List<ParamType> wrParams = wr.getParams();
			// build pipeline steps
			Step step = of.createStep();
			boolean first = true;
			for (Service svc : workflow.getServices()) {
				if (!first) {
					if (wr.getNextStep() == null) {	// make sure top level is set on request
						wr.setNextStep(step);
					}
					else {
						Step tmp = of.createStep();
						step.setNextStep(tmp);
						step = tmp;
					}
					step.setWorkQueue(svc.getWorkQueue());
					step.setType(svc.getInputType());
				}
				else {
					first = false;
				}
				// accumulate params from all of the services
				for (ParamType p : svc.getParams()) {
					wrParams.add(p);
				}
			}
			step.setNextStep(null);	// null out last step, filled in for next loop iteration

			for (MetaFile file : files) {
				long startTime = System.currentTimeMillis();
				if (file.file != null) {
					// put file in S3 input bucket
					String s3Key = MD5Util.md5Sum(new FileInputStream(file.file));
					RestS3Service s3 = new RestS3Service(new AWSCredentials(awsAccessId, awsSecretKey));
					if (proxyHost != null && !proxyHost.equals("")) {
						s3.initHttpProxy(proxyHost, proxyPort);
					}
					S3Object obj = new S3Object(new S3Bucket(inputBucket), s3Key);
					obj.setDataInputFile(file.file);
					obj.setContentLength(file.file.length());
					obj.setContentType(file.mimeType);
					obj = s3.putObject(inputBucket, obj);
					file.key = s3Key;
				}
				// send work request message
				FileRef ref = of.createFileRef();
				ref.setKey(file.key);
				ref.setType(file.mimeType);
				ref.setLocation("");
				wr.setInput(ref);
				long endTime = System.currentTimeMillis();
				String message = JAXBuddy.serializeXMLString(WorkRequest.class, wr);
				QueueUtil.sendMessageForSure(workQueue, message);
				// send work status message
				WorkStatus ws = MessageHelper.createIngestStatus(wr, file, startTime, endTime, "localhost");
				message = JAXBuddy.serializeXMLString(WorkStatus.class, ws);
				QueueUtil.sendMessageForSure(statusQueue, message);
				logger.debug("ingested : "+((file.file==null)?file.key:file.file.getName()));
			}
		} catch (IOException ex) {
			logger.error("Problem ingesting! "+ex.getMessage(), ex);
		} catch (Exception ex) {
			logger.error("Problem ingesting! "+ex.getMessage(), ex);
		}
	}
}
