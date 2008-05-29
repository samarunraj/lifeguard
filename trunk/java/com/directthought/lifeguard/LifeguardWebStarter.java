
package com.directthought.lifeguard;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.xerox.amazonws.common.JAXBuddy;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

import com.directthought.lifeguard.jaxb.PoolConfig;

/**
 * Runs lifeguard from a servlet context.
 */
public class LifeguardWebStarter implements ServletContextListener {
	private static Log logger = LogFactory.getLog(LifeguardWebStarter.class);

	private PoolSupervisor superVisor;
	private StatusLogger statLog;

	public void contextInitialized(ServletContextEvent evt) {
		try {
			XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("beans.xml"));
			PropertyPlaceholderConfigurer cfg = new PropertyPlaceholderConfigurer();
			cfg.setLocation(new ClassPathResource("aws.properties"));
			cfg.postProcessBeanFactory(factory);

			// Load pool configuration first - fail early if it isn't available
			PoolConfig config = JAXBuddy.deserializeXMLStream(PoolConfig.class,
					evt.getServletContext().getResourceAsStream("/WEB-INF/classes/poolconfig.xml"));

			// start status logger
			statLog = (StatusLogger)factory.getBean("statuslogger");
			Thread statusThread = new Thread(statLog);
			statusThread.start();

			// start pool manager(s)
			superVisor = (PoolSupervisor)factory.getBean("supervisor");
			superVisor.setPoolConfig(config);
			superVisor.setBeanFactory(factory);
			superVisor.run();

			Object mgr = factory.getBean("datamanager");
			evt.getServletContext().setAttribute("datamanager", mgr);
		} catch (JAXBException ex) {
			logger.error("Error parsing config file", ex);
		} catch (Throwable t) {
			logger.error("Uncaught error in spring/lifeguard initialization", t);
		}
	}

	public void contextDestroyed(ServletContextEvent evt) {
		try {
			superVisor.shutdown();
			statLog.shutdown();
		} catch (Throwable t) {
			logger.error("Uncaught error in spring/lifeguard shutdown", t);
		}
	}
}
