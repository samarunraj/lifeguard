
package com.directthought.lifeguard;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Properties;

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
public class LifeguardWebStarterEnv implements ServletContextListener {
	private static Log logger = LogFactory.getLog(LifeguardWebStarterEnv.class);

	private PoolSupervisor superVisor;
	private StatusLogger statLog;

	public void contextInitialized(ServletContextEvent evt) {
		try {
			XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("beans.xml"));
			PropertyPlaceholderConfigurer cfg = new PropertyPlaceholderConfigurer();
			Properties props = new Properties();
			props.put("aws.accessId", getGlobalStringResource("aws.accessId"));
			props.put("aws.secretKey", getGlobalStringResource("aws.secretKey"));
			props.put("aws.secret.accessId", getGlobalStringResource("aws.secret.accessId"));
			props.put("aws.secret.secretKey", getGlobalStringResource("aws.secret.secretKey"));
			props.put("aws.double.secret.accessId", getGlobalStringResource("aws.double.secret.accessId"));
			props.put("aws.double.secret.secretKey", getGlobalStringResource("aws.double.secret.secretKey"));
			props.put("aws.queuePrefix", getGlobalStringResource("aws.prefix"));
			props.put("proxy.host", getGlobalStringResource("proxy.host"));
			props.put("proxy.port", getGlobalStringResource("proxy.port"));
			cfg.setProperties(props);
			cfg.postProcessBeanFactory(factory);

			// Load pool configuration first - fail early if it isn't available
			PoolConfig config = getPoolConfig(evt.getServletContext().getResourceAsStream("/WEB-INF/classes/poolconfig.xml"));

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

	// this is put into a getter so that this class can be overridden by a custom class that tweaks
	// the service pool config to override some params programatically.
	protected PoolConfig getPoolConfig(InputStream configStream) throws JAXBException {
		return JAXBuddy.deserializeXMLStream(PoolConfig.class, configStream);
	}

    private String getGlobalStringResource(String propertyKey) {
        try {
            Context initContext = new InitialContext();
            Context context = (Context)initContext.lookup("java:comp/env");
            return (String)context.lookup(propertyKey);
        } catch (Exception ex) {
            logger.error("Couldn't locate JNDI resource : "+propertyKey);
        }
        return "";
    }
}
