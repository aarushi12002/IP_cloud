package msleepEngine.utils;



public class Constants {
	public static final TypedContext dbprop = new TypedContext();
	public static final String ERROR_DESCRIPTION = "Error Description : ";
	public static final String BUCKET_NAME = "msleep_bucket";
	public static final String DESTINATION_DIRECTORY = "src/main/";
	

	static {
		dbprop.setValue("cassandra.cluster", AppConfig.getHandle().getString("cassandra.cluster", "msleep"));
		dbprop.setValue("cassandra.host", AppConfig.getHandle().getString("cassandra.host","localhost"));
		dbprop.setValue("cassandra.user",AppConfig.getHandle().getString("cassandra.user", "cassandra")); 
		dbprop.setValue("cassandra.password",AppConfig.getHandle().getString("cassandra.password", "cassandra"));
		dbprop.setValue("cassandra.keyspace",AppConfig.getHandle().getString("cassandra.keyspace", "msleep"));
	}

}
