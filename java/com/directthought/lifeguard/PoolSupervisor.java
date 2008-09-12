
package com.directthought.lifeguard;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanFactory;

import com.directthought.lifeguard.jaxb.PoolConfig;

public class PoolSupervisor implements Runnable {
	private static Log logger = LogFactory.getLog(PoolSupervisor.class);

	private PoolConfig config;
	private List<PoolManager> pools;
	private BeanFactory factory;

	public PoolSupervisor() {
		pools = new ArrayList<PoolManager>();
	}

	public void setPoolConfig(PoolConfig config) {
		this.config = config;
	}

	public void setBeanFactory(BeanFactory factory) {
		this.factory = factory;
	}

	public List<PoolManager> getPoolManagers() {
		return this.pools;
	}

	public void run() {
		try {
			List<PoolConfig.ServicePool> configs = config.getServicePools();
			ThreadPoolExecutor pool = new ThreadPoolExecutor(configs.size(), configs.size(), 5,
											TimeUnit.SECONDS, new ArrayBlockingQueue(30));
			pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						logger.warn("failed to start pool manager!");
						//executor.execute(r);
					}
				});
			for (PoolConfig.ServicePool poolCfg : configs) {
				PoolManager pm = (PoolManager)factory.getBean("manager");
				pm.setPoolConfig(poolCfg);
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
