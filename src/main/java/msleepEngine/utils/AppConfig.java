package msleepEngine.utils;

import java.io.File;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;

public class AppConfig {
	public static CompositeConfiguration config = new CompositeConfiguration();
	private static Logger log = Logger.getLogger(AppConfig.class);

	static {
		config.addConfiguration(new SystemConfiguration());
		try{
			System.out.println(System.getProperty("user.dir"));
			File file = new File("src/main/resource/msleep.conf"); //TODO : Should be Configurable 
			if (file.exists()) {
				System.out.println();
				log.info("Loading User Properties");}
			config.append(new PropertiesConfiguration(file));
			System.out.println("CONFIG "+ config.getString("account.id"));
		} catch (Exception e) {
			log.info("No User specific Properties found to load");
		}
					
	}
	
	public static Configuration getHandle(){
		return config;
	}
}
