
package com.directthought.lifeguard.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class StatusCollectorManager {
	private Vector<PoolStatusCollector> tmpList;
	private HashMap<String, PoolStatusCollector> pools;

	public StatusCollectorManager() {
		tmpList = new Vector<PoolStatusCollector>();
	}

	public void addPoolCollector(PoolStatusCollector pool) {
		tmpList.add(pool);
	}

	public List<String> getPoolNames() {
		if (pools == null) {
			transferObjs();
		}
		List<String> ret = new ArrayList<String>(pools.keySet());
		Collections.sort(ret);
		return ret;
	}

	public Map<String, PoolStatusCollector> getPools() {
		if (pools == null) {
			transferObjs();
		}
		return pools;
	}

	private void transferObjs() {
		pools = new HashMap<String, PoolStatusCollector>();
		for (PoolStatusCollector pool : tmpList) {
			pools.put(pool.getServiceName(), pool);
		}
	}
}
