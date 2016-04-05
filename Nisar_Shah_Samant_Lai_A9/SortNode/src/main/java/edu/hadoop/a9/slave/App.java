package edu.hadoop.a9.slave;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

import edu.hadoop.a9.config.Configuration;

public class App {
	public static void main(String[] args) {
		log.info("Application Initialized");
		Configuration config = Configuration.getConfiguration();
		printConfiguration(config);
		
		log.info("Application Finished");
	}
	
	public static void sendSampleDistribution() {
		try{
		} catch (Exception exp) {
			StringWriter sw = new StringWriter();
			exp.printStackTrace(new PrintWriter(sw));
			log.severe(String.format("Error sending sampled distribution : %s", exp.getMessage()));
			log.severe(sw.toString());
		}
	}
	
	public static void printConfiguration(Configuration config) {
		String[] keys = {
			"aws.accesskey","aws.secretkey","client.hostname","client.portno","app.bucketname","app.inputprefix"
		};
		for(String key : keys){
			System.out.println(String.format("%s => %s", key, config.getProperty(key)));
		}
	}
	
	private static final Logger log = Logger.getLogger(App.class.getName());
}
