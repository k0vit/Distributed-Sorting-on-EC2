package neu.edu.mr.client;

import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import neu.edu.mr.utility.S3Service;

public class Client {
	private final static Logger LOG = Logger.getLogger("Client");
	private final static String SAMPLE_FILE_URL = "/samples";
	private final static String DISTRIBUTION_URL = "/partitions";
	private final static String SIGNAL_URL = "/signals";
	private final static String FILES_URL = "/files";
	private final static String DEFAULT_PORT = "4567";
	private static int request_count = 0;
	private static int signal_count = 0;
	public static int SLAVE_NUM;
	public static int FILE_NUM;
	public static long startTime;
	private static ArrayList<Distribution> samples = new ArrayList<>();
	private static ArrayList<String> slaves;
	private static S3Service s3;
	
	
	public static void main(String[] args) throws UnirestException, IOException, InterruptedException {
		if (args.length != 5) {
			System.err.println(
					"Usage: Client <input s3 path> <output s3 path> <config file path s3> <aws access key> <aws secret key>");
			for (int i = 0; i < args.length; i++) {
				System.err.println(args[i]);
			}
			System.err.println(args.length);
			System.exit(-1);
		}
		startTime = System.currentTimeMillis();
		
//		String configFilePath = args[2];
		// Download file from S3
//		BasicAWSCredentials awsCred = new BasicAWSCredentials(args[3], args[4]);
//		S3Service awsAgent = new S3Service(awsCred);
//		readConfigInfo(awsAgent.getObjectInputStream(configFilePath));
		
		/*
		 * initialize aws cred and S3 component
		 */
		BasicAWSCredentials awsCred = new BasicAWSCredentials(args[3],args[4]);
		s3 = new S3Service(awsCred);
		
		/*
		 * read config files
		 */
		readConfigInfo(args[2]);
		
		// TODO Code to send file names to each sort node. /files post request
		// Done
		distributeJob(args[0]);


		/*
		 * listen to /samples
		 */
		post(SAMPLE_FILE_URL, (req, res) -> {
			res.status(200);
			res.body("SUCCESS RECEIVE SAMPLE");
			request_count++;
			LOG.log(Level.FINE, req.body().toString());
			samples.add(new Distribution(req.body().toString()));
			// TODO delete
			slaves.add(req.ip());
			if (request_count == FILE_NUM) {
				postResult(samples);
			}

			return res.body().toString();
		});
		
		// TODO post method to get signal from sort node that its done and to
		// calculate timing.
		// System.println() to the console with t
		// Done
		post(SIGNAL_URL, (req,res) -> {
			res.status(200);
			res.body("SUCCESS SIGNAL RECEIVED");
			signal_count++;
			LOG.log(Level.FINE, "Sort work done signal from: "+req.ip());
			if (signal_count == SLAVE_NUM) {
				long totalTime = System.currentTimeMillis()-startTime;
				System.out.println("Sort Job Done");
				System.out.println("Total Time: " + totalTime/1000+" Seconds");
			}
			
			return res.body().toString();
		});

	}
	
	protected static void distributeJob(String inputS3Path){
		List<String> files = s3.getListOfObjects(inputS3Path);
		FILE_NUM = files.size();
		List<String> shares = divideJobs(files);
		for(int i = 0;i<SLAVE_NUM;i++){
			String slaveIp = slaves.get(i);
			String filesShare = shares.get(i);
			try{
				Unirest.post("http://" + slaveIp + ":" + DEFAULT_PORT + FILES_URL).body(filesShare).asString();
			} catch (Exception e){
				LOG.log(Level.SEVERE,"Failed to distribute files to slave: "+e.getMessage());
			}
		}
	}
	
	protected static List<String> divideJobs(List<String> files) {
		List<String> shares = new ArrayList<>();
		int share = FILE_NUM/SLAVE_NUM;
		int mod = FILE_NUM%SLAVE_NUM;
		for(int j=0,i=0;j<SLAVE_NUM;j++){
			StringBuilder sb = new StringBuilder();
			for (int k = i; i - k < share && i < FILE_NUM; i++) {
				sb.append(",");
				sb.append(files.get(i));
			}
			if (j<mod){
				sb.append(",");
				sb.append(files.get(i++));
			}
			sb.deleteCharAt(0);
			shares.add(sb.toString());
		}
		return shares;
	}
	
	
	/**
	 * read config file 
	 * @param configFilePath
	 * @param accessKey
	 * @param secretKey
	 * @throws IOException
	 */
	protected static void readConfigInfo(String configFilePath) throws IOException{
		readConfigInfo(s3.getObjectInputStream(configFilePath));
	}
	
	
	/**
	 * read config file and get slaves addr
	 * 
	 * @return a list of the slaves addr
	 * @throws IOException
	 */
	protected static void readConfigInfo(InputStream input) throws IOException {
		slaves = new ArrayList<>();
		try (InputStream fileInputStream = input) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] column = line.split(",");
				if (column[3].equals("S")) {
					slaves.add(column[1]);
				}
			}
			SLAVE_NUM = slaves.size();
		}
	}

	// TODO SLAVES list change
//	private static String[] readConfigFileForInformation(String configFileName) throws IOException {
//		List<String> ipList = new ArrayList<String>();
//		FileReader fr = new FileReader(configFileName);
//		BufferedReader br = new BufferedReader(fr);
//		String line = null;
//		while ((line = br.readLine()) != null) {
//			String[] column = line.split(",");
//			if (column[3].equals("S")) {
//				ipList.add(column[1]);
//			}
//		}
//		SLAVE_NUM = ipList.size();
//		return (String[]) ipList.toArray();
//	}

	/**
	 * post result to each slave
	 * 
	 * @param samples
	 */
	public static void postResult(ArrayList<Distribution> samples) {
		JSONObject obj = calculateDistribution(samples);
		String ret = obj.toJSONString();
		LOG.log(Level.INFO, "partitions: " + ret);
		for (String slaveIp : slaves) {
			try {
				Unirest.post("http://" + slaveIp + ":" + DEFAULT_PORT + DISTRIBUTION_URL).body(ret).asString();
			} catch (UnirestException e) {
				LOG.log(Level.SEVERE, "UNABLE TO POST RESULT TO SLAVE: " + slaveIp);
			}
		}
	}

	/**
	 * calculate the distribution from all slaves
	 * 
	 * @param dis
	 * @return
	 */
	// TODO remove min,max and for last node max =LONG.MAX and for first node
	// min = LONG.MIN
	// DONE
	public static JSONObject calculateDistribution(ArrayList<Distribution> dis) {
		ArrayList<Long> samples = new ArrayList<>();
		for (Distribution distribution : dis) {
			samples.addAll(distribution.getSamples());
		}
		return partition(samples, slaves);
	}

	/**
	 * partition the data to several job
	 * 
	 * @param samples
	 * @param slaves
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject partition(ArrayList<Long> samples, ArrayList<String> slaves) {
		Collections.sort(samples);
		int length = samples.size();
		JSONObject obj = new JSONObject();
		JSONArray arr = new JSONArray();
		int range = samples.size() / slaves.size();
		try {
			for (int i = 0, node = 0; i < length && node < slaves.size(); node++) {
				JSONObject job = new JSONObject();
				int endpos = Math.min(length - 1, i + range);
				long end = samples.get(endpos);

				if (node == 0)
					job.put("min", Long.MIN_VALUE);
				else
					job.put("min", samples.get(i));

				if (node == slaves.size() - 1)
					job.put("max", Long.MAX_VALUE);
				else
					job.put("max", end);
				job.put("nodeIp", slaves.get(node));
				// TODO remove
				// AS discussed, should keep this for final output sequence
				job.put("instanceId", node);
				arr.add(job);
				i += range;
				while (i < length && samples.get(i) == end)
					i++;
			}
		} catch (JSONException e) {
			LOG.log(Level.SEVERE, e.getMessage());
			LOG.log(Level.SEVERE, "failed to genearte global distribution");
		}
		obj.put("partitions", arr);
		return obj;
	}
}
