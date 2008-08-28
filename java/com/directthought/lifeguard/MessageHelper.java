
package com.directthought.lifeguard;

import java.math.BigInteger;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.directthought.lifeguard.jaxb.FileRef;
import com.directthought.lifeguard.jaxb.InstanceStatus;
import com.directthought.lifeguard.jaxb.ObjectFactory;
import com.directthought.lifeguard.jaxb.ParamType;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.WorkStatus;

public class MessageHelper {
	private static Log logger = LogFactory.getLog(MessageHelper.class);
	private static ObjectFactory of = new ObjectFactory();
	private static DatatypeFactory df = null;

	public static WorkStatus createIngestStatus(WorkRequest wr,
						MetaFile input, long startTime, long endTime, String instance) {
		WorkStatus ret = of.createWorkStatus();
		ret.setProject(wr.getProject());
		ret.setBatch(wr.getBatch());
		ret.setServiceName(wr.getServiceName());
		ret.setInputBucket(wr.getInputBucket());
		FileRef ref = of.createFileRef();
		if (input.file == null) {
			ref.setKey(input.key);
			ref.setType(input.mimeType);
		}
		else {
			ref.setLocation(input.file.getName());
		}
		ret.setInput(ref);
		ret.setOutputBucket(wr.getOutputBucket());
		ret.getOutputs().add(wr.getInput());
		List<ParamType> params = ret.getParams();
		for (ParamType p : wr.getParams()) {
			params.add(p);
		}
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeInMillis(startTime);
		ret.setStartTime(getDataFactory().newXMLGregorianCalendar(gc));
		gc.setTimeInMillis(endTime);
		ret.setEndTime(getDataFactory().newXMLGregorianCalendar(gc));

		return ret;
	}

	// TODO: refactor... 

	public static WorkStatus createServiceStatus(WorkRequest wr,
						List<MetaFile> outFiles, long startTime, long endTime, String instance) {
		WorkStatus ret = of.createWorkStatus();
		ret.setProject(wr.getProject());
		ret.setBatch(wr.getBatch());
		ret.setServiceName(wr.getServiceName());
		ret.setInputBucket(wr.getInputBucket());
		FileRef ref = of.createFileRef();
		ref.setKey(wr.getInput().getKey());
		ref.setType(wr.getInput().getType());
		ref.setLocation(wr.getInput().getLocation());
		ret.setInput(ref);
		ret.setOutputBucket(wr.getOutputBucket());
		if (outFiles != null) {
			for (MetaFile file : outFiles) {
				ref = of.createFileRef();
				ref.setKey(file.key);
				ref.setType(file.mimeType);
				ref.setLocation("S3");
				ret.getOutputs().add(ref);
			}
		}
		List<ParamType> params = ret.getParams();
		for (ParamType p : wr.getParams()) {
			params.add(p);
		}
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeInMillis(startTime);
		ret.setStartTime(getDataFactory().newXMLGregorianCalendar(gc));
		gc.setTimeInMillis(endTime);
		ret.setEndTime(getDataFactory().newXMLGregorianCalendar(gc));
		ret.setInstanceId(instance);

		return ret;
	}

	public static InstanceStatus createInstanceStatus(String instanceId, boolean busy, int dutyCycle) {
		InstanceStatus ret = of.createInstanceStatus();
		ret.setInstanceId(instanceId);
		ret.setState(busy?"busy":"idle");
		ret.setDutyCycle(new BigInteger(""+dutyCycle));
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeInMillis(System.currentTimeMillis());
		ret.setTimestamp(getDataFactory().newXMLGregorianCalendar(gc));

		return ret;
	}

	private static DatatypeFactory getDataFactory() {
		if (df == null) {
			try {
				df = DatatypeFactory.newInstance();
			} catch (DatatypeConfigurationException ex) {
				logger.error("Major JVM config issue : "+ex);
				System.exit(-1);	// need to exit. check the jvm config
			}
		}
		return df;
	}
}
