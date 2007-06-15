
package com.directthought.lifeguard;

import java.util.List;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.directthought.lifeguard.jaxb.PoolConfig;

public class PoolSupervisor implements Runnable {
	private static Log logger = LogFactory.getLog(PoolSupervisor.class);

	private String awsAccessId;
	private String awsSecretKey;
	private String queuePrefix;
	private PoolConfig config;


	public PoolSupervisor(String awsAccessId, String awsSecretKey, String queuePrefix, PoolConfig config) {
		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
		this.queuePrefix = queuePrefix;
		this.config = config;
	}

	public void run() {
		try {
			ThreadPoolExecutor pool = new ThreadPoolExecutor(20, 30, 5,
											TimeUnit.SECONDS, new ArrayBlockingQueue(30));
			pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						// just retry it
						//executor.execute(r);
					}
				});
			List<PoolConfig.ServicePool> configs = config.getServicePools();
			for (PoolConfig.ServicePool poolCfg : configs) {
				pool.execute(new PoolManager(awsAccessId, awsSecretKey, queuePrefix, poolCfg));
			}
		} catch (Throwable t) {
			logger.error(t);
		}
	}
}
