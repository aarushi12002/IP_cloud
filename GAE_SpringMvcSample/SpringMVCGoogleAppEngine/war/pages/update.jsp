<%@ page import="appEngine.model.Sensor" %>
<%@ page import="com.google.appengine.api.datastore.KeyFactory" %>
<html>
<body>
	<h1>Update sensor</h1>
	
	<%
		Sensor sensor = new Sensor();
	
		if(request.getAttribute("sensor")!=null){
		
			sensor = (Sensor)request.getAttribute("sensor");
			
		}
		
	%>
	
	<form method="post" action="../update" >
		<input type="hidden" name="key" id="key" 
			value="<%=KeyFactory.keyToString(sensor.getKey()) %>" /> 
		
		<table>
			<tr>
				<td>
					Uuid :
				</td>
				<td>
					<input type="text" style="width: 185px;" maxlength="30" name="uuid" id="uuid" 
						value="<%=sensor.getUuid() %>" />
				</td>
			</tr>
			<tr>
				<td>
					Value :
				</td>
				<td>
					<input type="text" style="width: 185px;" maxlength="30" name="value" id="value" 
						value="<%=sensor.getValue() %>" />
				</td>
			</tr>
		</table>
		<input type="submit" class="update" title="Update" value="Update" />
	</form>
	
</body>
</html>