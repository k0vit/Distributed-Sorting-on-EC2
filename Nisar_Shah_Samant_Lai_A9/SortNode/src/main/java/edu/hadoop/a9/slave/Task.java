package edu.hadoop.a9.slave;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import edu.hadoop.a9.common.ClientNodeCommWrapper;
import edu.hadoop.a9.common.Distribution;
import edu.hadoop.a9.common.S3Wrapper;
import edu.hadoop.a9.config.Configuration;

public class Task implements Runnable {
	public Task(String id, String objectId, S3Wrapper s3client) {
		this.id = id;
		this.s3client = s3client;
		this.objectId = objectId;
		this.config = Configuration.getConfiguration();
		this.client = new ClientNodeCommWrapper();
		this.bucketName = this.config.getProperty("app.bucketname");
	}
	
	public void run() {
		try {
			Distribution dist = GetDistribution();
			if( dist != null )
				client.SendData(dist.toJson());
		} catch ( Exception exp ) {
			StringWriter sw = new StringWriter();
			exp.printStackTrace(new PrintWriter(sw));
			log.severe(String.format("Error sending sampled distribution : %s", exp.getMessage()));
			log.severe(sw.toString());
		}
	}
	
	public Distribution GetDistribution() throws Exception {
		InputStream is = s3client.getObjectInputStream(bucketName, objectId);
		is = new GZIPInputStream(is);
		Scanner in = new Scanner(is);
		int minTemp = Integer.MAX_VALUE;
		int maxTemp = Integer.MIN_VALUE;
		int samplesTaken = 0;
		int totalSamplesToTake = config.getIntProperty("app.totaldatasamples");
		Random rnd = new Random();
		Distribution dist = new Distribution();
		
		while(in.hasNextLine()){
			String line = in.nextLine();
			String[] parts = line.split("\\,");
			int temp = Integer.parseInt(parts[BULBTEMP_INDEX]);
			if( temp < minTemp ) minTemp = temp;
			if( temp > maxTemp ) maxTemp = temp;
			if( samplesTaken < totalSamplesToTake && rnd.nextBoolean() ){
				dist.getSamples().add(temp);
			}
		}
		
		in.close();
		dist.setMaxTemp(maxTemp);
		dist.setMinTemp(minTemp);
		log.info(String.format("%s: %s/%s => %s", id, bucketName, objectId, dist));
		return dist;
	}
	
	private final String id;
	private final S3Wrapper s3client;
	private final String objectId;
	private final String bucketName;
	private final Configuration config;
	private final ClientNodeCommWrapper client;
	private static final Logger log = Logger.getLogger(Task.class.getName());
	private static final int BULBTEMP_INDEX = 8;
}
