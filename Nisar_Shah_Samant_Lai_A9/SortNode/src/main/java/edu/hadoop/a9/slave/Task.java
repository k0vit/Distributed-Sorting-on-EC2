package edu.hadoop.a9.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.hadoop.a9.common.NodeCommWrapper;
import edu.hadoop.a9.common.S3Wrapper;

/**
 * This is the thread task which will download the files it is assigned from the
 * client in a multithreaded way.
 * 
 * @author Naineel Shah
 * @author Kovit Nisar
 *
 */
public class Task implements Runnable {
	private final String filename;
	private static final Logger log = Logger.getLogger(Task.class.getName());
	private static final int BULBTEMP_INDEX = 8;
	// Using these many data points for sampling
	private static final int TOTAL_DATA_SAMPLES = 80000;
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

	/**
	 * Downloads each file in the local file system. Samples the given files and
	 * send the distribution to the client to create partitions.
	 */
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

	/**
	 * For a given file, Sample the file and create the sampled data as a String
	 * separated by ,
	 * 
	 * @param fileName
	 * @return
	 */
	public String GetDistribution(String fileName) {
		File file = new File(System.getProperty("user.dir") + File.separator + fileName);
		log.info(String.format("[%s] Get distribution for file: %s", fileName, file.getAbsolutePath()));
		StringBuilder commaSeparatedString = new StringBuilder();
		Random rnd = new Random();
		String sampledData = null;
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
			int samplesTaken = 0;
			int totalSamplesToTake = TOTAL_DATA_SAMPLES;
			String line = null;
			br.readLine();
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("\\,");
				if ((parts.length < 9) || parts[BULBTEMP_INDEX].equals("-")) {
					continue;
				}
				try {
					if (samplesTaken < totalSamplesToTake && rnd.nextBoolean()) {
						commaSeparatedString.append(parts[BULBTEMP_INDEX]);
						commaSeparatedString.append(",");
						samplesTaken++;
					}
				} catch (Exception e) {
					log.severe("Exception parsing data: " + e.getMessage());
					continue;
				}
			}
		} catch (Exception e) {
			log.severe("Exception while reading data. Reason:" + e.getMessage());
			commaSeparatedString = null;
			return "";
		}

		log.info(String.format("File: %s is now sampled.", fileName));
		commaSeparatedString.deleteCharAt(commaSeparatedString.length() - 1);
		sampledData = commaSeparatedString.toString();
		commaSeparatedString = null;
		return sampledData;
	}
}
