
package com.directthought.lifeguard.status;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.directthought.lifeguard.StatusSaver;
import com.directthought.lifeguard.jaxb.FileRef;
import com.directthought.lifeguard.jaxb.WorkStatus;

public class CSVLogger implements StatusSaver {
	private static Log logger = LogFactory.getLog(CSVLogger.class);
	private BufferedOutputStream outStr;

	public void setOutputFile(String file) {
		try {
			File f = new File(file);
			boolean writeHeader = !f.exists();
			outStr = new BufferedOutputStream(new FileOutputStream(f, true));	// append=yes
			if (writeHeader) {
				String header = "Project, Batch, ServiceName, InputBucket, InputKey, InputType, "+
								"OutputBucket, OutputKey, OutputType, StartTime, EndTime, InstanceId, ServiceFailure\n";
				outStr.write(header.getBytes());
			}
		} catch (IOException ex) {
			logger.error("Error opening csv file for output : "+file, ex);
		}
	}

	public void workStatus(WorkStatus ws) {
		StringBuilder line = new StringBuilder();
		line.append(ws.getProject());
		line.append(",");
		line.append(ws.getBatch());
		line.append(",");
		line.append(ws.getServiceName());
		line.append(",");
		line.append(ws.getInputBucket());
		line.append(",");
		FileRef ref = ws.getInput();
		line.append(ref.getKey());
		line.append(",");
		line.append(ref.getType());
		line.append(",");
		line.append(ws.getOutputBucket());
		line.append(",");
		ref = ws.getOutputs().isEmpty()?new FileRef():ws.getOutputs().get(0);	// only logging the first file... 
		line.append(ref.getKey());
		line.append(",");
		line.append(ref.getType());
		line.append(",");
		line.append(ws.getStartTime().toString());
		line.append(",");
		line.append(ws.getEndTime().toString());
		line.append(",");
		line.append(ws.getInstanceId());
		line.append(",");
		String failureMsg = ws.getFailureMessage();
		if (failureMsg != null) {
			line.append(failureMsg);
		}
		line.append("\n");
		try {
			outStr.write(line.toString().getBytes());
			outStr.flush();
		} catch (IOException ex) {
			logger.error("Problem writing status message to file.", ex);
		}
	}
}
