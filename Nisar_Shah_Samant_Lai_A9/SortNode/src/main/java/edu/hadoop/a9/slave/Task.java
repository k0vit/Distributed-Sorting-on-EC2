package edu.hadoop.a9.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.hadoop.a9.common.NodeCommWrapper;
import edu.hadoop.a9.common.S3Wrapper;

public class Task implements Runnable {
	private final String filename;
	private static final Logger log = Logger.getLogger(Task.class.getName());
	private static final int BULBTEMP_INDEX = 8;
	// Using 10% of 300000 data for sampling
	private static final int TOTAL_DATA_SAMPLES = 1;
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
			log.info(String.format("[%s] Downloaded File successfully from S3", fileName));
			String jsonDist = GetDistribution(fileName);
			if (jsonDist != null) {
				log.info(String.format("Sending samples for file: %s", fileName));
				NodeCommWrapper.SendData(clientIp, CLIENT_PORT, SAMPLE_URL, jsonDist, fileName);
			}
		} catch (Exception exp) {
			log.severe(String.format("Error sending sampled distribution : %s", exp.getMessage()));
		}
	}

	public String GetDistribution(String fileName) {
		File file = new File(System.getProperty("user.dir") + File.separator + fileName);
		log.info(String.format("[%s] Get distribution for file: %s", fileName, file.getAbsolutePath()));
//		JSONObject mainObject = new JSONObject();
		StringBuilder commaSeparatedString = new StringBuilder();
		Random rnd = new Random();
//		JSONArray array = new JSONArray();
		BufferedReader br = null;
		try {
			
			InputStream is = new FileInputStream(file);
			is = new GZIPInputStream(is);
			br = new BufferedReader(new InputStreamReader(is));
		}
		catch (Exception e) {
			log.severe("Failed while buffering file " + file + ". Reason " + e.getLocalizedMessage() 
					+ ". Cause " + e.getCause());
			
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			log.severe("Stacktrace: " + errors.toString());
			return commaSeparatedString.toString();
//			mainObject.put("samples", array);
//			return mainObject.toJSONString();
		}
		String line = null;
		try {
			int samplesTaken = 0;
			int totalSamplesToTake = TOTAL_DATA_SAMPLES;
			//Ignore first line which is the header.
			br.readLine();
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("\\,");
				if ((parts.length < 9) || parts[BULBTEMP_INDEX].equals("-")) {
					continue;
				}
				try {
					double temp = Double.parseDouble(parts[BULBTEMP_INDEX]);
					if (samplesTaken < totalSamplesToTake && rnd.nextBoolean()) {
//						array.add(temp);
						commaSeparatedString.append(temp);
						commaSeparatedString.append(",");
						samplesTaken++;
					}
				} catch (Exception e) {
					log.severe("Exception parsing data: " + e.getMessage());
					continue;
				}
			}
			br.close();
		} catch (Exception e) {
			log.severe("Exception while reading data: " + line + ". Reason:"+ e.getMessage());
			return commaSeparatedString.toString();
//			mainObject.put("samples", array);
//			return mainObject.toJSONString();
		}
		log.info(String.format("File: %s is now sampled.", fileName));
//		mainObject.put("samples", array);
//		return mainObject.toJSONString();
		commaSeparatedString.substring(0, commaSeparatedString.length() - 1);
		return commaSeparatedString.toString();
	}
}
