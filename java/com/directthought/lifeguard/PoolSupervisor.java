
package com.directthought.lifeguard;

import java.util.ArrayList;
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
	private List<PoolManager> pools;

	public PoolSupervisor(String awsAccessId, String awsSecretKey, String queuePrefix, PoolConfig config) {
		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
		this.queuePrefix = queuePrefix;
		this.config = config;
		pools = new ArrayList<PoolManager>();
	}

	public void run() {
		try {
			ThreadPoolExecutor pool = new ThreadPoolExecutor(20, 30, 5,
											TimeUnit.SECONDS, new ArrayBlockingQueue(30));
			pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						//executor.execute(r);
					}
				});
			List<PoolConfig.ServicePool> configs = config.getServicePools();
			for (PoolConfig.ServicePool poolCfg : configs) {
				PoolManager pm = new PoolManager(awsAccessId, awsSecretKey, queuePrefix, poolCfg);
				pool.execute(pm);
				pools.add(pm);
			}
			pool.shutdown();	// pre-emptive
		} catch (Throwable t) {
			logger.error("something went horribly wrong when the pool supervisor tried to run pool managers!", t);
		}
	}

	public void shutdown() {
		for (PoolManager pm : pools) {
			pm.shutdown();
		}
	}
}
