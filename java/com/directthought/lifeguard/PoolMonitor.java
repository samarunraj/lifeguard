
package com.directthought.lifeguard;

/**
 * This interface describes a listener that gets updated by the PoolManager while the pool
 * is running. Configure your pool monitor in the beans.xml
 */
public interface PoolMonitor {

	/**
	 * This is called when an instance is started.
	 *
	 * @param id the instance id
	 */
	public void instanceStarted(String id);

	/**
	 * This is called when an instance is terminated.
	 *
	 * @param id the instance id
	 */
	public void instanceTerminated(String id);

	/**
	 * This is called when an instance reports busy.
	 *
	 * @param id the instance id
	 * @param loadEstimate how busy the instance is
	 */
	public void instanceBusy(String id, int loadEstimate);

	/**
	 * This is called when an instance reports idle.
	 *
	 * @param id the instance id
	 * @param loadEstimate how busy the instance is
	 */
	public void instanceIdle(String id, int loadEstimate);

	/**
	 * This is called when an instance hasn't reported status recently. If an instance
	 * has failed, this will likely be called several times before that instance is
	 * termianted and replaced. There is a chance this may be called for an instance that
	 * is temporarily unavailable for whatever reason.
	 *
	 * @param id the instance id
	 */
	public void instanceUnresponsive(String id);

	// these 3 methods are here simply for a server simulater used in testing
	public void setServiceName(String name);

	public void setStatusQueue(String name);

	public void setWorkQueue(String name);
}
