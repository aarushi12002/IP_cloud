package msleepEngine.csvReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import msleepEngine.cassandra.DatabaseHandle;
import msleepEngine.utils.AppConfig;
import msleepEngine.utils.TypedContext;

import au.com.bytecode.opencsv.CSVReader;

public class CSVLoader_Android {
	Logger logger = LoggerFactory.getLogger(CSVLoader.class);
	private final String[] Billed_Header = {"user_id","timestamp", "acc_x"
	                                        ,"acc_y","acc_z","airflow","spo","pulserate"};
	private final int[] filterIndices={1,2,3,4,5,6};
	Pattern pattern = Pattern
			.compile(".*[\\/]([0-9]*)-.*tags-([0-9]*-[0-9]*).csv");

	CSVReader reader;
	String[] header = null;
	
	/**
	 *this is the path to the externa storage where are the files are stored.
	 */
	String data_location = "";

	/**
	 * 
	 * @param readFile
	 *            file path.
         * @return
	 */
	public boolean CSVReaderforBill(String readFile) {
		//File sdcard = Environment.getExternalStorageDirectory();
		//File file = new File(sdcard, filePath);
		/*
		 *this is the path to the external storage where are the files are stored.
		 */
		String data_location = "";

		readFile = data_location + readFile;
		logger.info("Reading " + readFile);
		long count = 0;
		try {
			//separator tags - is an array of strings
			reader = new CSVReader(new FileReader(readFile), ',', '"');
			// Header details reading
			header = reader.readNext();
			readFile = readFile.substring(0, readFile.lastIndexOf("."));
			readFile = readFile.replaceAll("[-+.^:,/]", "");
			// Trim the file // why?
			if (readFile.length() > 48) {
				readFile = readFile.substring(readFile.length() - 48,
						readFile.length ());
			}

			

			// TODO: don't see the point here?
		/*	for (Integer filter : filterSet) {
				if (!Billed_Header[filter - 1]
						.equalsIgnoreCase(header[filter - 1])) {
					logger.info(String.format("Loaded %d rows from %s", count,
							readFile));
					return true;
				}
			}*/
			
			// Data reading and update in to cassandra
			for (Object[] data : reader.readAll()) {
				filteredWrite(header, data, "sensorvalue");
				count++;
			}

			DatabaseHandle.getDatabase().commitPending();

			Thread.sleep(5000);

		} catch (IOException e) {
			System.out.print(e);
		} catch (InterruptedException e) {
			System.out.println(e);
		}

		return true;
	}

	private void filteredWrite(String[] header, Object[] data,
			 String table) {
		TypedContext context = new TypedContext();
		TypedContext con = new TypedContext();

		try {
			for (int i=0; i<6;i++) {
				context.setValue(TypedContext.normalizeKey(header[i]),
						data[i]);
			}

			// user_id
			if (context.containsKey("user_id")) {
				int id = Integer.parseInt(context.get("user_id").toString());
				con.setValue("user_id", id, "Int");
			}
			// timestamp
			if (context.containsKey("timestamp")) {
				long timestamp = Long.parseLong(context.get("timestamp")
						.toString());
				con.setValue("timestamp", timestamp, "Long");
			}
			// x y z
			if (context.containsKey("acc_x")) {
				float acc_x = Float.parseFloat(context.get("acc_x").toString());
				con.setValue("acc_x", acc_x, "Float");
			}
			if (context.containsKey("acc_y")) {
				float acc_y = Float.parseFloat(context.get("acc_y").toString());
				con.setValue("acc_y", acc_y, "Float");
			}
			if (context.containsKey("acc_z")) {
				float acc_z = Float.parseFloat(context.get("acc_z").toString());
				con.setValue("acc_z", acc_z, "Float");
			}

			// spo
			if (context.containsKey("spo")) {
				int spo = Integer.parseInt(context.get("spo").toString());
				con.setValue("spo", spo, "Int");
			}

			// pulse
			if (context.containsKey("pulserate")) {
				int pulserate = Integer.parseInt(context.get("pulserate")
						.toString());
				con.setValue("pulserate", pulserate, "Int");
			}

			// airflow
			if (context.containsKey("airflow")) {
				int airflow = Integer.parseInt(context.get("airflow")
						.toString());
				con.setValue("airflow", airflow, "Int");
			}

			DatabaseHandle.getDatabase().write(table, con);
		} catch (Exception e) {
			e.printStackTrace();
			logger.warn("Error " + con);

		}

	}
}