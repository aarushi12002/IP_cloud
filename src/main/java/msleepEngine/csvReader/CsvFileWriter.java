package msleepEngine.csvReader;

import java.io.FileWriter;
import java.io.IOException;

/**
 * 
 * Class to write sensor values to .csv file
 * I've shown a commented sample to write to it 
 * for accelerometer sensor.
 * The advantage of .csv file is we needn't create separate 
 * files like .txt since all the fields are there as specified
 * by FILE_HEADER.
 *
 */
public class CsvFileWriter {

    //Delimiter used in CSV file
    private static final String COMMA_DELIMITER = ",";

    private static final String NEW_LINE_SEPARATOR = "\n";

    private static final String FILE_HEADER = "user_id, timestamp, "
    										+ "acc_x, acc_y, acc_z"
    										+ "airflow, spo, pulserate";

    /**
     * 
     * @param fileName:
     * 				we just need one file since it has fields 
     * 				for all the sensors.
     * 			
     */
    public static void writeCsvFile(String fileName) {


        FileWriter fileWriter = null;
        
        /*
         * code for writing to the .csv file, similar to writing to the text file
         * 
         */
        try{
        	fileWriter = new FileWriter(fileName);
        	
        	// only append it once
        	fileWriter.append(FILE_HEADER.toString());
        	
        	//this is a sample write for accelerator sensor
        	/*the default value for the fields not used i.e. 
        	 * airflow, spo, pulserate
        	 *  are 0
        	 * 
        	 *
        	 *	fileWriter.append(user_id.toString);
        	 *  fileWriter.append(COMMA_DELIMITER);
        	 * 	fileWriter.append(timestamp.toString);
        	 * fileWriter.append(COMMA_DELIMITER);
        	 * 	fileWriter.append(acc_x.toString);
        	 * fileWriter.append(COMMA_DELIMITER);
        	 * 	fileWriter.append(acc_y.toString);
        	 * fileWriter.append(COMMA_DELIMITER);
        	 * 	fileWriter.append(acc_z.toString);
        	 * fileWriter.append(NEW_LINE_SEPARATOR);
        	 */
        	
        } catch(IOException ioe){
        	System.err.println("Error in CSVFileWriter");
        } finally {
        	try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        }
        
}
}