package edu.hadoop.a9.slave;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.hadoop.a9.common.S3Wrapper;
import edu.hadoop.a9.config.Configuration;

public class App {
	public static void main(String[] args) {
		log.info("Application Initialized");
		Configuration config = Configuration.getConfiguration();
		printConfiguration(config);
		sendSampleDistribution(config);
		log.info("Application Finished");
	}
	
	public static void sendSampleDistribution(Configuration config) {
		try{
			String accessKey = config.getProperty("aws.accesskey");
			String secretKey = config.getProperty("aws.secretkey");
			String bucketName = config.getProperty("app.bucketname");
			String prefix = config.getProperty("app.inputprefix");
			
			ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();
			AmazonS3Client s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
			S3Wrapper wrapper = new S3Wrapper(s3client);
			
			List<String> objectIds = wrapper.getListOfObjects(bucketName, prefix);
			int totalSamples = config.getIntProperty("app.totalsamples");
			Random rnd = new Random();
			for( int i=0 ; i<totalSamples ; i++ ){
				int idx = rnd.nextInt();
				String objectId = objectIds.get(idx);
				log.info(String.format("Selected Object => %s", objectId));
				Task task = new Task("Task" +i, objectId, wrapper);
				executor.execute(task);
			}
			executor.shutdown();
		} catch (Exception exp) {
			StringWriter sw = new StringWriter();
			exp.printStackTrace(new PrintWriter(sw));
			log.severe(String.format("Error sending sampled distribution : %s", exp.getMessage()));
			log.severe(sw.toString());
		}
	}
	
	public static void printConfiguration(Configuration config) {
		String[] keys = {
			"aws.accesskey","aws.secretkey","client.url","app.bucketname","app.inputprefix",
			"app.totalsamples","app.totaldatasamples"
		};
		for(String key : keys){
			System.out.println(String.format("%s => %s", key, config.getProperty(key)));
		}
	}
	
	private static final Logger log = Logger.getLogger(App.class.getName());
}
