package msleepEngine.cassandra;

import msleepEngine.utils.Constants;

public class DatabaseHandle {
	private static final DataStaxCassandraHandler db = new DataStaxCassandraHandler(Constants.dbprop, true);
	
	public static DataStaxCassandraHandler getDatabase(){
		return db;
	}
}
