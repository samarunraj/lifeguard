
package com.directthought.lifeguard;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

import com.xerox.amazonws.common.JAXBuddy;

import com.directthought.lifeguard.jaxb.PoolConfig;

public class RunManager {
	private static Log logger = LogFactory.getLog(RunManager.class);

	public static void main(String [] args) {
		if (args.length != 1) {
			System.out.println("usage: RunManager <poolconfig.xml>");
		}

		XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("beans.xml"));
		PropertyPlaceholderConfigurer cfg = new PropertyPlaceholderConfigurer();
		cfg.setLocation(new ClassPathResource("aws.properties"));
		cfg.postProcessBeanFactory(factory);

		try {
			// start status logger
			StatusLogger statLog = (StatusLogger)factory.getBean("statuslogger");
			statLog.run();

			// start pool manager(s)
			PoolConfig config = JAXBuddy.deserializeXMLStream(PoolConfig.class,
											new FileInputStream(args[0]));
			PoolSupervisor superVisor = (PoolSupervisor)factory.getBean("supervisor");
			superVisor.setPoolConfig(config);
			superVisor.setBeanFactory(factory);
			superVisor.run();
			BufferedReader rdr = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				rdr.readLine();
				System.out.print("Do you want to exit? (Y/n) :");
				String line = rdr.readLine();
				if (!line.toLowerCase().equals("n")) {
					break;
				}
			}
			superVisor.shutdown();
		} catch (FileNotFoundException ex) {
			logger.error("Count not find config file : "+args[0], ex);
		} catch (IOException ex) {
			logger.error("Error reading config file", ex);
		} catch (JAXBException ex) {
			logger.error("Error parsing config file", ex);
		}
	}
}
