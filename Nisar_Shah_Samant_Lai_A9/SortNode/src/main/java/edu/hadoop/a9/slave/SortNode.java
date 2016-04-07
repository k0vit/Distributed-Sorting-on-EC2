package edu.hadoop.a9.slave;

import static spark.Spark.post;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.hadoop.a9.common.S3Wrapper;

public class SortNode {
	static String accessKey;
	static String secretKey;
	static String clientIp;
	
	public static void main(String[] args) {
		if (args.length != 5) {
			System.err.println("Usage: SortNode <input s3 path> <output s3 path> <config file path s3> <aws access key> <aws secret key>");
			for (int i = 0; i < args.length; i++) {
				System.err.println(args[i]);
			}
			System.err.println(args.length);
			System.exit(-1);
		}
		accessKey = args[3];
		secretKey = args[4];
		log.info("Application Initialized");
		sendSampleDistribution();
		
		
		log.info("Application Finished");
	}
	
	public static void sendSampleDistribution() {
		try {
			BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
			AmazonS3Client s3client = new AmazonS3Client(awsCredentials);
			S3Wrapper wrapper = new S3Wrapper(s3client);
			//TODO Read data from each file and store in memory in local directory.
			//TODO Client will send the file names that need to be downloaded from S3 and you need to download those files only.
			post("/files", (request, response) -> {
				//Receive request from client for the files which need to be taken care of
				clientIp = request.ip();
				response.status(200);
				response.body("SUCCESS");
				String fileString = request.body();
				String[] filenames = downloadAndStoreFileInLocal(wrapper, fileString, awsCredentials);
				randomlySample(filenames);
				return response.body();
			});

		} catch (Exception exp) {
			StringWriter sw = new StringWriter();
			exp.printStackTrace(new PrintWriter(sw));
			log.severe(String.format("Error sending sampled distribution : %s", exp.getMessage()));
			log.severe(sw.toString());
		}
	}
	
	/**
	 * This method takes the string of filenames, creates separate threads and sends each file sampling to client Node. 
	 * @param filenames
	 */
	private static void randomlySample(String[] filenames) {
		ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();
		for (String filename : filenames) {
			Task task = new Task(filename, clientIp);
			executor.execute(task);
		}
		executor.shutdown();
	}

	private static String[] downloadAndStoreFileInLocal(S3Wrapper wrapper, String fileString, BasicAWSCredentials awsCredentials) {
		String[] files = fileString.split(",");
		for (String file : files) {
			try {
				wrapper.readOutputFromS3(file, awsCredentials);
			} catch (IOException | InterruptedException e) {
				log.severe(e.getMessage());
			}
		}
		return files;
	}

	private static final Logger log = Logger.getLogger(SortNode.class.getName());
}
