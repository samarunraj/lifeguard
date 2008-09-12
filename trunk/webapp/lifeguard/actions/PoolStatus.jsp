<%@ page import="
			java.util.ArrayList,
			com.directthought.lifeguard.monitor.StatusCollectorManager,
			com.directthought.lifeguard.monitor.PoolStatusCollector,
			com.directthought.lifeguard.monitor.PoolStatusCollector.PoolEvent" %>
<pool>
<%
	String serviceName = request.getParameter("serviceName");
    PoolStatusCollector pool = ((StatusCollectorManager)getServletContext().getAttribute("datamanager")).getPools().get(serviceName);
	if (pool != null) { %>
		<serviceName><%=pool.getServiceName()%></serviceName>
		<serversRunning><%=pool.getServersRunning()%></serversRunning>
		<serversBusy><%=pool.getServersBusy()%></serversBusy>
		<poolLoad><%=pool.getPoolLoad()%></poolLoad>
		<eventLog>
			<% for (PoolEvent evt : pool.getEventList()) { %>
				<event>
					<eventType><%=evt.getType().label()%></eventType>
					<instanceId><%=evt.getInstanceId()%></instanceId>
					<timestamp><%=evt.getTimestamp()%></timestamp>
				</event>
			<%} %>
		</eventLog>
		<serverHistory>
			<% ArrayList<String> running = pool.getRunningHistory();
			   ArrayList<String> busy = pool.getBusyHistory();
			   for (int i=0 ; i<running.size(); i++) { %>
				<report running="<%=running.get(i)%>" busy="<%=busy.get(i)%>"/>
			<%} %>
		</serverHistory>
	<%}
%>
</pool>
