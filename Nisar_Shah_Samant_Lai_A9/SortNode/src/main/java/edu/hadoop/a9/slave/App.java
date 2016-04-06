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

//TODO we dont need configuration you will get 5 arguments as 
//<input s3 path> <output s3 path> <config file path s3> <aws access key> <aws secret key>
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
			
			//TODO Read data from each file and store in memory in local directory.
			//TODO Client will send the file names that need to be downloaded from S3 and you need to download those files only.
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
	
	//TODO dont need it
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
