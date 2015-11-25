package msleepEngine.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.append;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.util.concurrent.ListenableFuture;

import msleepEngine.utils.AppConfig;
import msleepEngine.utils.TypedContext;




/**
 * 
 * DataStaxCassandraHandler
 *     Connect/Read/Write [Cluster.Keyspace].Table
 * 
 * 
 *
 *
 */

public class DataStaxCassandraHandler {

	private static Logger logger = LoggerFactory.getLogger(DataStaxCassandraHandler.class);
	private AtomicInteger  batch_count = new AtomicInteger(0);
	private Batch batch = QueryBuilder.batch();
	private Cluster cluster = null;
	private Session session = null;
	private boolean isConnected = false;

	public DataStaxCassandraHandler(TypedContext context){
		init(context, true);
	}
	
	public DataStaxCassandraHandler(TypedContext context, boolean isAuth){
		init(context, isAuth);
	}
	
	private boolean init(TypedContext context, boolean isAuth){
		String strCluster = (String)context.getValue("cassandra.cluster");
		String[] strHostPort = context.getValue("cassandra.host").toString().split(",");
		
		try {
			if (strCluster != null && strHostPort != null)
				if (isAuth) {
					String user = (String)context.getValue("cassandra.user");
					String pass = (String)context.getValue("cassandra.password");
				    cluster = Cluster.builder()
		    				.withClusterName(strCluster)
		    				.addContactPoints(strHostPort)
		    				.withCredentials(user, pass)
		    				.build();
				} else {
				    cluster = Cluster.builder()
				    				.withClusterName(strCluster)
				    				.addContactPoints(strHostPort)
				    				.build();				      
				}
			isConnected = true;
			String keyspaceName = (String)context.getValue("cassandra.keyspace");
			session = cluster.connect(keyspaceName);
			if (session != null)
				logger.info(String.format ("DB Connected %s.%s", session.getCluster().getClusterName(), 
												session.getLoggedKeyspace()));
			else 
				logger.info(String.format ("DB Connection failed (%s)", context.toString()));
		} catch (Exception e) {
			logger.info("Exception C*Handler creation "+ e.getMessage());
			return false;
		}	
		return true;
	}

	public boolean isConnected() { return isConnected; }

	public void close(){
		cluster.close();
	}


	public void update(String table, TypedContext context, TypedContext where) {
		//default CL set to Quorum
		this.update(table, context,where, ConsistencyLevel.valueOf(AppConfig.getHandle().getString("ConsistencyLevel")));
		
	}


	public void updateSingle(String table, TypedContext context, TypedContext where) {
		//default CL set to Quorum
		this.updateBatch(table, context,where, ConsistencyLevel.valueOf(AppConfig.getHandle().getString("ConsistencyLevel")), -1, -1);
		
	}

	public void update(String table, TypedContext context, TypedContext where, ConsistencyLevel consistency, int ttl) {
		this.updateBatch(table, context, where, consistency, 
				AppConfig.getHandle().getInt("cassandra.batch"), ttl);
	}

	
	public void update(String table, TypedContext context, TypedContext where, ConsistencyLevel consistency) {
		this.updateBatch(table, context, where, consistency, 
				AppConfig.getHandle().getInt("cassandra.batch"), -1);
	}
	
	public void commitPending(){
		this.updateBatch(null, null, null, null,-1, -1);
	}
	
	
	@SuppressWarnings("unchecked")
	public void updateBatch(String table, TypedContext context, TypedContext where, ConsistencyLevel consistency, int batch_size, int ttl) {
		if (table != null) { //Force Commit all pending 
			Update update = QueryBuilder.update(table);
			if (ttl != -1)	update.using(QueryBuilder.ttl(ttl));
			for (String key : context.getKeySet()){
				if (context.getType(key).equalsIgnoreCase("List")) //TODO support other type too
					update.with(append(key, context.get(key)));
				else if (context.getType(key).equalsIgnoreCase("Map")) 
					update.with(QueryBuilder.putAll(key, (Map<String, String>)context.get(key)));
				else if (context.getType(key).equalsIgnoreCase("Set")) 
					update.with(QueryBuilder.addAll(key, (Set<String>)context.get(key)));
				else
					update.with(set(key, context.get(key)));
			}
	
			for (String key : where.getKeySet()){
				update.where(eq(key, where.get(key)));
			}
			batch.setConsistencyLevel(consistency);
			batch.add(update); ; batch_count.incrementAndGet();
		}
		if (batch_count.get() >= batch_size || batch_size == -1) {
			logger.info(String.format("Commiting batch of size %d", batch_count.get()));
			ResultSet rs = getSession().execute(batch);
			if (rs != null && logger.isInfoEnabled()) {
				ExecutionInfo executionInfo = rs.getExecutionInfo();
				if (logger.isDebugEnabled())
					logger.debug(String.format ("Update Executed@ %s", 
							executionInfo.getQueriedHost().getAddress().getHostAddress()));
			}
			batch_count.set(0);
			batch = QueryBuilder.batch();
		}
	}
	
	public void write(String table, TypedContext context, ConsistencyLevel consistency) {
		this.write(table, context, consistency, -1);
		
	}
	
	public void write(String table, TypedContext context) {
		//default CL set to Quorum
		this.write(table, context, ConsistencyLevel.valueOf(AppConfig.getHandle().getString("ConsistencyLevel")), -1);
		
	}

	public void write(String table, TypedContext context, int ttl) {
		//default CL set to Quorum
		this.write(table, context, ConsistencyLevel.valueOf(AppConfig.getHandle().getString("ConsistencyLevel")), ttl);
		
	}
	
	
	public void write(String table, TypedContext context, ConsistencyLevel consistency, int ttl) {
		//keyspace = null , table= sensor_name
		Insert insert = QueryBuilder.insertInto(table);
                //adds the key value pair of "TTL", ttl
		// value of timestamp 
		if(ttl != -1) insert.using(QueryBuilder.ttl(ttl));
		insert.setConsistencyLevel(consistency);
		for (String name : context.getKeySet()) {
			Object value = context.get(name);
			if (value != null){
				//logger.info(String.format("Writing %s.%s,%s",name,context.getType(name), value));
				insert.value(name, value);
			}
		}
		ResultSet rs = getSession().execute(insert);
		if (rs != null && logger.isInfoEnabled()) {
			ExecutionInfo executionInfo = rs.getExecutionInfo();
			/**
			 * Basic information on the execution of a query.
			 */
			if (logger.isDebugEnabled())
				logger.debug(String.format ("Insert Executed@ %s", 
						executionInfo.getQueriedHost().getAddress().getHostAddress()));
		}
	}

	/*
	*This is for the range queries on cassandra tables, most importantly to get the results for a time period.
	*/
	
	public ResultSet rangeQueries(String table, String key, String lowerLimit, String upperLimit){

		Select query = QueryBuilder
			      .select()
			      .from(table);
		 	query.where(QueryBuilder.gte(key, lowerLimit)).and(QueryBuilder.gte(key, upperLimit));
		 	return getSession().execute(query);
		 	
	}
	
	public List<TypedContext> readWithFilteredColumns(String table, String key, String value, String... columns) {
		ResultSet rows = this.dtReadFilteredColumns(table, key, value, columns);
		return rs2context(rows);
	}

	
	public List<TypedContext> readWithFilterRows(String table, String key, String value) {
		ResultSet  rows = this.dtReadFilteredRows(table, key, value);
		return rs2context(rows);
	}

	public List<TypedContext> readColumns(String table, String... columns) {
		ResultSet  rows = this.dtReadColumns(table, columns);
		return rs2context(rows);
	}

	
	public List<TypedContext> readRows(String table) {
		ResultSet rows = this.dtReadRows(table);
		return rs2context(rows);
	}

	
	public Session getSession(){
		return session;
	}

	
	public List <TypedContext> executeQuery(String table,TypedContext where, String... columns){
		Select select = QueryBuilder.select(columns).from(table);
		
		for (String key : where.getKeySet()){
			select.where(eq(key, where.get(key)));
		}
		
		ResultSet rs = getSession().execute(select);
		return rs2context(rs);
	}
	

	
	
	public List <TypedContext> executeQuery(String query){

		Statement toPrepare = new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(AppConfig
				.getHandle().getString("ConsistencyLevel.read")));
		ResultSet rs  = getSession().execute(toPrepare);
		return rs2context(rs);
	}
	
	public List <TypedContext> executeQuery(String query, String cl){

		Statement toPrepare = new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(cl));
		ResultSet rs  = getSession().execute(toPrepare);
		return rs2context(rs);
	}
	
	
	public List <TypedContext> executeQuery(String query, int page, int pageSize){
		List <TypedContext> paged = new ArrayList <TypedContext>();
		Statement stmt = new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(AppConfig.getHandle()
				.getString("ConsistencyLevel.read")));
		stmt.setFetchSize(pageSize); 
		ResultSet result = session.execute(stmt); 
		for(int i=1; i<page; i++){
			if (result.isFullyFetched()) {
				logger.info("All Page read");
				return  new ArrayList <TypedContext>(); //Return Empty Set to avoid NPE
			}
			ListenableFuture<Void>  ft = result.fetchMoreResults();
			while(!ft.isDone()); // Wait till its done
		}
		int start = (pageSize * (page-1))+1;
		int end =  result.getAvailableWithoutFetching();
		int counter = 1;
		logger.info(String.format("Reading Records %d : %d\n", start, end));
		for (Row row : result){
			if (counter >= start && counter <= end){
				paged.add(this.row2context(row));
			}else if (counter > end){
				break;
			}
			counter ++;
		}
		return paged;
	}


	
	
	public void create(String table, TypedContext tc){
		String schema = "";
		for (String column : tc.getSchema()){
			//logger.info(String.format("Creating %s.%s,%s",column, tc.getType(column), tc.get(column)));
			schema += String.format("%s %s,", column, tc.getDBType(column));
		}
		String creator = String.format("CREATE TABLE IF NOT EXISTS %s (%s PRIMARY KEY (ID));", table, schema);
		logger.info(creator);
		this.executeQuery(creator);
	}

		
	public List<TypedContext> rs2context(ResultSet rows){
		List<TypedContext> result = new ArrayList<TypedContext>();
		for (Row row : rows){
			 List<Definition> defs = row.getColumnDefinitions().asList();
			 //TODO : Support multiple Types, currently only String and List, Map
			 TypedContext context = new TypedContext();
			 for (Definition def : defs){
				 String col = def.getName();
				 String type = def.getType().asJavaClass().getSimpleName();
				 Object val = this.format(row, col, type);
				 context.setValue(col, val);
			 }
			 result.add(context);
		 }
		return result;
	}
	

	public TypedContext row2context(Row row) {
		List<Definition> defs = row.getColumnDefinitions().asList();
		// TODO : Support multiple Types, currently only String and List, Map
		TypedContext context = new TypedContext();
		for (Definition def : defs) {
			String col = def.getName();
			String type = def.getType().asJavaClass().getSimpleName();
			Object val = this.format(row, col, type);
			context.setValue(col, val);
		}
		return context;
	}

	
	private ResultSet dtReadFilteredColumns(String table, String key, String value, String... columns) {
		
		 	Select query = QueryBuilder
			      .select(columns)
			      .from(table);
		 	
		 	query.where(QueryBuilder.eq(key, value));
	      	
		  return getSession().execute(query);
	}

	
	private ResultSet dtReadColumns(String table, String... columns) {
		
	 	Select query = QueryBuilder
		      .select(columns)
		      .from(table);
      	
	  return getSession().execute(query);
	}

	private ResultSet dtReadFilteredRows(String table, String key, String value) {
		
	 	Select query = QueryBuilder
		      .select()
		      .from(table);
	 	
	 	query.where(QueryBuilder.eq(key, value));
      	
	  return getSession().execute(query);
	}

	private ResultSet dtReadRows(String table) {
		
	 	Select query = QueryBuilder
		      .select()
		      .from(table);
	 	      	
	  return getSession().execute(query);
	}

	
	
	private Object format(Row row, String column, String type){
		
		//TODO: Lot more support types there on Cassandra
		
		switch (type){
		case "Long":
			return row.getLong(column);
		case "String":
			return row.getString(column);
		case "Map":
			return row.getMap(column, String.class, String.class);
		case "List":
			return row.getList(column, String.class);
		case "Set":
			return row.getSet(column, String.class);
		case "Boolean":
			return row.getBool(column);
		case "Date":
			return row.getDate(column);
		case "Float":
			return row.getFloat(column);
		case "Double":
			return row.getDouble(column);
		}
		
		return null;
	}
	
//	public static void main(String argv[]){
//		logger.info("Testing here");
//		TypedContext context = new TypedContext();
//		context.setValue("cassandra.cluster", "minjar");
//		context.setValue("cassandra.host", "localhost");
//		context.setValue("cassandra.user", "minjar");
//		context.setValue("cassandra.password", "minjar");
//		context.setValue("cassandra.keyspace", "\"LogData\"");
//		DataStaxCassandraHandler dsch = new DataStaxCassandraHandler(context, true);
//		logger.info(dsch.readColumns("tester", "first_name", "last_name").toString());
//		
//		TypedContext data = new TypedContext();
//		data.setValue("user_id", "user");
//		data.setValue("first_name", "First");
//		data.setValue("last_name", "Last");
//		data.setValue("leader", false);
//		data.setValue("book", new ArrayList<String>() {/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//		{
//		    add("Book1");
//		    add("Book2");
//		    add("Book3");
//		}});
//		data.setValue("emails", new HashSet<String>() {/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//		{
//		    add("self@minjar.com");
//		    add("admin@minjar.com");
//		}});
//		data.setValue("emails", new HashSet<String>() {/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//		{
//		    add("self@minjar.com");
//		    add("admin@minjar.com");
//		}});
//		data.setValue("prop", new HashMap<String, String>() {/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//		{
//		    put("1", "self@minjar.com");
//		    put("2", "admin@minjar.com");
//		}});
//
//		dsch.write("tester", data, ConsistencyLevel.ANY);
//		
//		TypedContext updater = data;
//		updater.remove("user_id");
//		data.setValue("book", "Book10", "List");
//
//		TypedContext where = new TypedContext();
//		where.setValue("user_id", "gdurai");
//
//		dsch.update("tester", updater, where);
//
//		
//		logger.info(dsch.readRows("tester").toString());
//
//		dsch.close();
//		System.out.println("Read : " + DatabaseHandle.getDatabase().executeQuery("select id, timestamp from ec2",11, 100).toString());
//	}
}
