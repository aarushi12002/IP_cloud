package msleepEngine.csvReader;

import java.io.BufferedReader;
import java.io.FileReader;


import msleepEngine.cassandra.DatabaseHandle;
import msleepEngine.utils.TypedContext;

/**
 * 
 * class to read the a .csv file and upload the sensor values to the 
 * corresponding sensor tables in cassandra.
 *
 */
public class CsvFileReader {
	
	 private static final String COMMA_DELIMITER = ",";

	 private static final String NEW_LINE_SEPARATOR = "\n";
	 
	 private static final int user_id = 0;
	 private static final int timestamp = 1;
	 private static final int acc_x = 2;
	 private static final int acc_y = 3;
	 private static final int acc_z = 4;
	 private static final int airflow = 5;
	 private static final int spo = 6;
	 private static final int pulserate = 7;
	 


	public static void readCsvFile(String fileName) {
		BufferedReader reader = null;
		// name of the cassandra table.
		String table="";
		try {
			reader = new BufferedReader(new FileReader(fileName));
			// this is to skip the header/ fields in the csv file
			reader.readLine();
			
			
			String row = "";
			while((row = reader.readLine())!=null) {
				String[] tokens = row.split(COMMA_DELIMITER);
				
				if(tokens.length > 0){
					TypedContext con = new TypedContext();
					con.setValue("user_id", tokens[user_id]);
					con.setValue("timestamp", Long.parseLong(tokens[timestamp]));
					/*
					 *just check the type once if it is double or float 
					 */
					if(Double.parseDouble(tokens[acc_x])!=0) {
						con.setValue("acc_x", Double.parseDouble(tokens[acc_x]));
						con.setValue("acc_y", Double.parseDouble(tokens[acc_y]));
						con.setValue("acc_z", Double.parseDouble(tokens[acc_z]));
						table = "accelerometer";
						
					} else if(Double.parseDouble(tokens[airflow])!=0) {
						con.setValue("airflow", Double.parseDouble(tokens[airflow]));
						table = "airflow";
					} else if(Double.parseDouble(tokens[spo])!=0) {
						con.setValue("spo", Double.parseDouble(tokens[spo]));
						table = "spo";
					} else if(Double.parseDouble(tokens[pulserate])!=0) {
						con.setValue("pulserate", Double.parseDouble(tokens[pulserate]));
						table = "pulserate";
					}
					DatabaseHandle.getDatabase().write(table, con);
				}
			}
			
		} catch( Exception e) {
			e.printStackTrace();
		} finally {
			
		}
		
		
	}
}
