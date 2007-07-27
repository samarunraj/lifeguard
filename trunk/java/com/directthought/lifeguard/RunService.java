
package com.directthought.lifeguard;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.common.JAXBuddy;

import com.directthought.lifeguard.jaxb.ServiceConfig;

public class RunService {
	private static Log logger = LogFactory.getLog(RunService.class);

	public static void main(String [] args) {
		if (args.length != 2) {
			System.out.println("usage: RunService <serviceClass> <serviceconfig.xml>");
			System.exit(-1);
		}
		try {
			Properties props = new Properties();
			props.load(RunService.class.getClassLoader().getResourceAsStream("aws.properties"));
			String accessId = props.getProperty("aws.accessId");
			String secretKey = props.getProperty("aws.secretKey");
			String queuePrefix = props.getProperty("aws.queuePrefix");

			ServiceConfig config = JAXBuddy.deserializeXMLStream(ServiceConfig.class,
											new FileInputStream(args[1]));
			Class svcClass = Class.forName(args[0]);
			Constructor c = svcClass.getConstructor(new Class [] {
								ServiceConfig.class, String.class, String.class, String.class});
			AbstractBaseService svc = (AbstractBaseService)c.newInstance(new Object [] {
								config, accessId, secretKey, queuePrefix});
			logger.debug("loaded "+args[0]+", going to invoke it.");
			svc.run();
		} catch (FileNotFoundException ex) {
			logger.error("Counld not find config file : "+args[1], ex);
		} catch (ClassNotFoundException ex) {
			logger.error("Counld not load class : "+args[0], ex);
		} catch (NoSuchMethodException ex) {
			logger.error("Service class must implement base class contructor", ex);
		} catch (InstantiationException ex) {
			logger.error("Error in constructor for : "+args[0], ex);
		} catch (IllegalAccessException ex) {
			logger.error("Is service class contructor public?", ex);
		} catch (InvocationTargetException ex) {
			logger.error("Error in constructor for : "+args[0], ex);
		} catch (IOException ex) {
			logger.error("Error reading config file", ex);
		} catch (JAXBException ex) {
			logger.error("Error parsing config file", ex);
		}
	}
}
