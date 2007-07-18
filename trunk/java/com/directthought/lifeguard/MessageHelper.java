
package com.directthought.lifeguard;

import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import com.directthought.lifeguard.jaxb.FileRef;
import com.directthought.lifeguard.jaxb.ObjectFactory;
import com.directthought.lifeguard.jaxb.WorkRequest;
import com.directthought.lifeguard.jaxb.WorkStatus;

public class MessageHelper {

	public static WorkStatus createWorkStatus(WorkRequest wr,
						String inputFile, long startTime, long endTime, String instance) throws DatatypeConfigurationException {
		ObjectFactory of = new ObjectFactory();
		WorkStatus ret = of.createWorkStatus();
		ret.setProject(wr.getProject());
		ret.setBatch(wr.getBatch());
		ret.setServiceName(wr.getServiceName());
		ret.setInputBucket(wr.getInputBucket());
		FileRef ref = of.createFileRef();
		ref.setKey("");
		ref.setType("");
		ref.setLocation(inputFile);
		ret.setInput(ref);
		ret.setOutputBucket(wr.getOutputBucket());
		ret.getOutputs().add(wr.getInput());
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeInMillis(startTime);
		ret.setStartTime(DatatypeFactory.newInstance().newXMLGregorianCalendar(gc));
		gc.setTimeInMillis(endTime);
		ret.setEndTime(DatatypeFactory.newInstance().newXMLGregorianCalendar(gc));

		return ret;
	}
}
