<%@ page 
   import="java.util.Iterator,
           java.util.List,
           com.directthought.lifeguard.monitor.StatusCollectorManager,
           com.directthought.lifeguard.monitor.PoolStatusCollector,
           com.directthought.lifeguard.monitor.PoolStatusCollector.PoolEvent" %>
<%
    List pools = ((StatusCollectorManager)getServletContext().getAttribute("datamanager")).getPoolNames();
%>
<pools>
<% for (Iterator iter = pools.iterator(); iter.hasNext();) {
	String pool = (String)iter.next(); %>
	<pool><%=pool%></pool>
<% } %>
</pools>
