
package com.directthought.lifeguard.monitor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.directthought.lifeguard.PoolMonitor;

public class PoolStatusCollector implements PoolMonitor {
	private String serviceName;
	private ArrayList<PoolEvent> eventLog;
	private int serversRunning;
	private int serversBusy;
	private Map<String, Integer> instanceLoad;
	private ArrayList<String> runningHistory;
	private ArrayList<String> busyHistory;

	public PoolStatusCollector() {
		eventLog = new ArrayList<PoolEvent>();
		instanceLoad = new HashMap<String, Integer>();
		runningHistory = new ArrayList<String>();
		busyHistory = new ArrayList<String>();
		// the code below creates fake data for use in testing
//		new HistoryMaker().start();
//		eventLog.add(new PoolEvent(EventType.instanceStarted, "i-abcdefg"));
//			try { Thread.sleep(1000); } catch (InterruptedException ex) {}
//		eventLog.add(new PoolEvent(EventType.instanceBusy, "i-abcdefg"));
//			try { Thread.sleep(1000); } catch (InterruptedException ex) {}
//		eventLog.add(new PoolEvent(EventType.instanceIdle, "i-abcdefg"));
//			try { Thread.sleep(1000); } catch (InterruptedException ex) {}
//		eventLog.add(new PoolEvent(EventType.instanceBusy, "i-abcdefg"));
//			try { Thread.sleep(1000); } catch (InterruptedException ex) {}
//		eventLog.add(new PoolEvent(EventType.instanceIdle, "i-abcdefg"));
	}

	public void setDataManager(StatusCollectorManager manager) {
		manager.addPoolCollector(this);
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String name) {
		serviceName = name;
	}

	public void setStatusQueue(String name) {
	}

	public void setWorkQueue(String name) {
	}

	public void instanceStarted(String id) {
		System.err.println(">>>>>>>>>>> instance started : "+id);
		eventLog.add(new PoolEvent(EventType.instanceStarted, id));
		instanceLoad.put(id, 0);
		serversRunning++;
	}

	public void instanceTerminated(String id) {
		System.err.println(">>>>>>>>>>> instance terminated : "+id);
		eventLog.add(new PoolEvent(EventType.instanceStopped, id));
		instanceLoad.remove(id);
		serversRunning--;
	}

	public void instanceBusy(String id, int loadEstimate) {
		System.err.println(">>>>>>>>>>> instance busy : "+id+" load : "+loadEstimate);
		eventLog.add(new PoolEvent(EventType.instanceBusy, id));
		Integer load = instanceLoad.get(id);
		if (load != null) {
			instanceLoad.put(id, loadEstimate);
		}
		serversBusy++;
		if (serversBusy > serversRunning) serversBusy = serversRunning;
	}

	public void instanceIdle(String id, int loadEstimate) {
		System.err.println(">>>>>>>>>>> instance idle : "+id+" load : "+loadEstimate);
		eventLog.add(new PoolEvent(EventType.instanceIdle, id));
		Integer load = instanceLoad.get(id);
		if (load != null) {
			instanceLoad.put(id, loadEstimate);
		}
		serversBusy--;
		if (serversBusy < 0) serversBusy = 0;
	}

	public void instanceUnresponsive(String id) {
		System.err.println(">>>>>>>>>>> instance unresponsive : "+id);
		eventLog.add(new PoolEvent(EventType.instanceUnresponsive, id));
	}

	public String getServersRunning() {
		return ""+serversRunning;
	}

	public String getServersBusy() {
		return ""+serversBusy;
	}

	public List<PoolEvent> getEventList() {
		return eventLog;
	}

	public int getPoolLoad() {
		if (instanceLoad.size() == 0) return 0;
		// else, calculate the value
		long total = 0;
		for (int i : instanceLoad.values()) {
			total += i;
		}
		return (int)(total / instanceLoad.size());
	}

	public ArrayList<String> getRunningHistory() {
		return runningHistory;
	}

	public ArrayList<String> getBusyHistory() {
		return busyHistory;
	}

	public enum EventType {
		instanceStarted ("Instance Started"),
		instanceStopped ("Instance Stopped"),
		instanceBusy ("Instance Busy"),
		instanceIdle ("Instance Idle"),
		instanceUnresponsive ("Instance Unresponsive");

		private final String label;

		EventType(String label) {
			this.label = label;
		}

		public String label() {
			return label;
		}
	}

	public class PoolEvent {
		private EventType type;
		private String instanceId;
		private Date timestamp;

		public PoolEvent(EventType type, String instanceId) {
			this.type = type;
			this.instanceId = instanceId;
			this.timestamp = new Date();
		}

		public EventType getType() {
			return type;
		}

		public String getInstanceId() {
			return instanceId;
		}

		public Date getTimestamp() {
			return timestamp;
		}
	}

	class HistoryMaker extends Thread {
		public void run() {
			while (true) {
				runningHistory.add(""+serversRunning);
				busyHistory.add(""+serversBusy);
				try { Thread.sleep(5000); } catch (InterruptedException ex) {}
				// do some random stuff to make history to test with
				serversRunning += Math.round((float)((Math.random()*2)-1.0));
				serversBusy += Math.round((float)((Math.random()*2)-1.0));
				if (serversRunning < 0) serversRunning = 0;
				if (serversBusy < 0) serversBusy = 0;
				if (serversBusy > serversRunning) serversBusy = serversRunning;
			}
		}
	}
}
