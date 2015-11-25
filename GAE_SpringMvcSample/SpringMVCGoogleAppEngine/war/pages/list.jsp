<%@ page import="java.util.List" %>
<%@ page import="appEngine.model.Sensor" %>
<%@ page import="com.google.appengine.api.datastore.KeyFactory" %>
<html>
<body>
	<h1>Uploading Sensor values to DataStore</h1>

	Function : <a href="add">Upload to DataStore</a>
	<hr />

	<h2>Sensor Values</h2>
	<table border="1">
		<thead>
			<tr>
				<td>TimeStamp</td>
				<td>Value1(x)</td>
				<td>Value2(y)</td>
				<td>Value3(z)</td>
			</tr>
		</thead>
		
		<%
			
			if(request.getAttribute("sensorList")!=null){
				
				List<Sensor> sensors = (List<Sensor>)request.getAttribute("sensorList");
				
				if(!sensors.isEmpty()){
					 for(Sensor c : sensors){
						 
		%>
					<tr>
					  <td><%=c.getDate() %></td>
					  <td><%=c.getUuid() %></td>
					  <td><%=c.getValue() %></td>
					  
					  <td><a href="update/<%=c.getUuid()%>">Update</a> 
		                   | <a href="delete/<%=KeyFactory.keyToString(c.getKey()) %>">Delete</a></td>
					</tr>
		<%	
			
					}
		    
				}
			
		   	}
		%>
         
        </tr>
     
	</table>

</body>
</html>