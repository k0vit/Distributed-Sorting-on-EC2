package edu.hadoop.a9.slave;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.hadoop.a9.common.NodeCommWrapper;
import edu.hadoop.a9.common.S3Wrapper;

public class Task implements Runnable {
	private final String filename;
	private static final Logger log = Logger.getLogger(Task.class.getName());
	private static final int BULBTEMP_INDEX = 8;
	//Using 10% of 300000 data for sampling
	private static final int TOTAL_DATA_SAMPLES = 30000;
	private final String clientIp;
	private static final String CLIENT_PORT = "4567";
	private static final String SAMPLE_URL = "samples";
	private final BasicAWSCredentials awsCredentials;
	private final String inputS3Path;
	
	public Task(String filename, String clientIp, BasicAWSCredentials awsCredentials, String inputS3Path) {
		this.filename = filename;
		this.clientIp = clientIp;
		this.awsCredentials = awsCredentials;
		this.inputS3Path = inputS3Path;
	}
	
	public void run() {
		try {
			AmazonS3Client s3client = new AmazonS3Client(awsCredentials);
			S3Wrapper wrapper = new S3Wrapper(s3client);
			String fileName = wrapper.downloadAndStoreFileInLocal(filename, awsCredentials, inputS3Path);
			String jsonDist = GetDistribution(fileName);
			if( jsonDist != null )
				NodeCommWrapper.SendData(clientIp, CLIENT_PORT, SAMPLE_URL, jsonDist);
		} catch ( Exception exp ) {
			StringWriter sw = new StringWriter();
			exp.printStackTrace(new PrintWriter(sw));
			log.severe(String.format("Error sending sampled distribution : %s", exp.getMessage()));
			log.severe(sw.toString());
		}
	}
	
	@SuppressWarnings("unchecked")
	public String GetDistribution(String fileName) throws Exception {
		File file = new File(System.getProperty("user.dir"), fileName);
		InputStream is = new FileInputStream(file);
		is = new GZIPInputStream(is);
		Scanner in = new Scanner(is);
		int samplesTaken = 0;
		int totalSamplesToTake = TOTAL_DATA_SAMPLES;
		Random rnd = new Random();
		JSONArray array = new JSONArray();
		while(in.hasNextLine()) {
			String line = in.nextLine();
			String[] parts = line.split("\\,");
			double temp = Double.parseDouble(parts[BULBTEMP_INDEX]);
			if( samplesTaken < totalSamplesToTake && rnd.nextBoolean() ){
				array.add(temp);
			}
		}
		in.close();
		log.info(String.format("This %s is now sampled.",filename));
		JSONObject mainObject = new JSONObject();
		mainObject.put("samples", array);
		return mainObject.toJSONString();
	}
	
}
