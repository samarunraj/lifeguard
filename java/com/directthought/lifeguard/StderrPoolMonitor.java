
package com.directthought.lifeguard;

// something dirt simple to test this spring bean
public class StderrPoolMonitor implements PoolMonitor {
	public void setServiceName(String name) {
		System.err.println(">>>>>>>>>>> service name : "+name);
	}

	public void setStatusQueue(String name) {
		System.err.println(">>>>>>>>>>> status queue name : "+name);
	}

	public void setWorkQueue(String name) {
		System.err.println(">>>>>>>>>>> work queue name : "+name);
	}

	public void instanceStarted(String id) {
		System.err.println(">>>>>>>>>>> instance started : "+id);
	}

	public void instanceTerminated(String id) {
		System.err.println(">>>>>>>>>>> instance terminated : "+id);
	}

	public void instanceBusy(String id) {
		System.err.println(">>>>>>>>>>> instance busy : "+id);
	}

	public void instanceIdle(String id) {
		System.err.println(">>>>>>>>>>> instance idle : "+id);
	}
}
