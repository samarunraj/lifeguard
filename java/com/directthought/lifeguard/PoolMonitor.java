
package com.directthought.lifeguard;

public interface PoolMonitor {
	public void setServiceName(String name);

	public void setStatusQueue(String name);

	public void setWorkQueue(String name);

	public void instanceStarted(String id);

	public void instanceTerminated(String id);

	public void instanceBusy(String id);

	public void instanceIdle(String id);
}
