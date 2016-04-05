package edu.hadoop.a9.config;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import java.util.logging.Logger;

public class Configuration {
	public Configuration() { 
		properties = new Properties();
	}
	
	public Configuration(InputStream is) {
		properties = new Properties();
		Load(is);
	}
	
	public void Load(InputStream is) {
		try {
			properties.load(is);
		} catch (Exception exp) {
			StringWriter sw = new StringWriter();
			exp.printStackTrace(new PrintWriter(sw));
			log.severe(String.format("Unable to load the properties : %s", exp.getMessage()));
			log.severe(sw.toString());
		}
	}
	
	public static synchronized Configuration getConfiguration() {
		if(config == null){
			config = new Configuration(Configuration.class.getResourceAsStream("app.properties"));
		}
		return config;
	}
	
	public String getProperty( String name ) {
		return properties.getProperty(name);
	}
	
	public int getIntProperty( String name ) {
		return Integer.parseInt(properties.getProperty(name));
	}

	private Properties properties;
	private static volatile Configuration config = null;
	private static final Logger log = Logger.getLogger(Configuration.class.getName());
}
