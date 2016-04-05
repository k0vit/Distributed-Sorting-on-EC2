package edu.hadoop.a9.slave;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

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
		return null;
	}
	
	private final String id;
	private final S3Wrapper s3client;
	private final String objectId;
	private final Configuration config;
	private final ClientNodeCommWrapper client;
	private static final Logger log = Logger.getLogger(Task.class.getName());
}
