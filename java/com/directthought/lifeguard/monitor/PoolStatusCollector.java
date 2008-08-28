
package com.directthought.lifeguard.monitor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.directthought.lifeguard.PoolMonitor;

public class PoolStatusCollector implements PoolMonitor {
	private String serviceName;
	private ArrayList<PoolEvent> eventLog;
	private int serversRunning;
	private int serversBusy;
	private ArrayList<String> runningHistory;
	private ArrayList<String> busyHistory;

	public PoolStatusCollector() {
		eventLog = new ArrayList<PoolEvent>();
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
		serversRunning++;
	}

	public void instanceTerminated(String id) {
		System.err.println(">>>>>>>>>>> instance terminated : "+id);
		eventLog.add(new PoolEvent(EventType.instanceStopped, id));
		serversRunning--;
	}

	public void instanceBusy(String id) {
		System.err.println(">>>>>>>>>>> instance busy : "+id);
		eventLog.add(new PoolEvent(EventType.instanceBusy, id));
		serversBusy++;
		if (serversBusy > serversRunning) serversBusy = serversRunning;
	}

	public void instanceIdle(String id) {
		System.err.println(">>>>>>>>>>> instance idle : "+id);
		eventLog.add(new PoolEvent(EventType.instanceIdle, id));
		serversBusy--;
		if (serversBusy < 0) serversBusy = 0;
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
		instanceIdle ("Instance Idle");

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
