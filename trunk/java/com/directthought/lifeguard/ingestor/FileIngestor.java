
package com.directthought.lifeguard.ingestor;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.common.JAXBuddy;

import com.directthought.lifeguard.IngestorBase;
import com.directthought.lifeguard.jaxb.Workflow;

public class FileIngestor extends IngestorBase {
	private static Log logger = LogFactory.getLog(FileIngestor.class);

	protected FileIngestor(String awsAccessId, String awsSecretKey, String queuePrefix,
							String project, String batch,
							String inputBucket, String outputBucket,
							String statusQueueName, Workflow workflow) {
		super(awsAccessId, awsSecretKey, queuePrefix, project, batch, inputBucket, outputBucket,
				statusQueueName, workflow);
	}
							 
	public static void main(String [] args) throws Exception {
		if (args.length < 6) {
			logger.error("Usage: FileIngestor <aws.props> <project> <batch> <bucket> <workflow.xml> file1 ...");
		}
		// load aws props
		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));

		// load workflow
		Workflow workflow = JAXBuddy.deserializeXMLStream(Workflow.class,
											new FileInputStream(args[4]));

		FileIngestor ingestor = new FileIngestor(
							(String)props.get("aws.accessId"), (String)props.get("aws.secretKey"),
							(String)props.get("aws.queuePrefix"),
							args[1], args[2], args[3], args[3],
							"status-input", workflow);

		ArrayList<File> files = new ArrayList<File>();
		for (int i=5; i<args.length; i++) {
			files.add(new File(args[i]));
		}
		ingestor.ingest(files);
	}
}
