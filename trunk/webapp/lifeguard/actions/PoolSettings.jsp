<%@ page import="
			java.util.ArrayList,
			com.directthought.lifeguard.jaxb.PoolConfig,
			com.directthought.lifeguard.PoolSupervisor,
			com.directthought.lifeguard.PoolManager" %>
<%
	String serviceName = request.getParameter("serviceName");
    PoolSupervisor sv = (PoolSupervisor)getServletContext().getAttribute("supervisor");
	if (sv != null) {
		for (PoolManager mgr : sv.getPoolManagers()) {
			if (mgr.getServiceName().equals(serviceName)) { %>
				<poolSettings>
					<serviceAMI><%=mgr.getServiceAMI()%></serviceAMI>
					<type><%=mgr.getInstanceType()%></type>
					<minSize><%=mgr.getMinimumSize()%></minSize>
					<maxSize><%=mgr.getMaximumSize()%></maxSize>
					<rampUpInt><%=mgr.getRampUpInterval()%></rampUpInt>
					<rampDownInt><%=mgr.getRampDownInterval()%></rampDownInt>
					<rampUpDelay><%=mgr.getRampUpDelay()%></rampUpDelay>
					<rampDownDelay><%=mgr.getRampDownDelay()%></rampDownDelay>
					<queueSizeFactor><%=mgr.getQueueSizeFactor()%></queueSizeFactor>
				</poolSettings>
				<% break;
			}
		}
	}
%>
