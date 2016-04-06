package neu.edu.mr.client;
import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
	private final static String DEFAULT_PORT = "4567";
	private final static String REGISTER_URL = "/register";
	private static int request_count = 0;
	public static int SLAVE_NUM;
	private static ArrayList<Distribution> samples = new ArrayList<>();
	private static ArrayList<String> slaves = new ArrayList<>();
	
	public static void main(String[] args) throws UnirestException, IOException, InterruptedException{
		if (args.length != 5) {
			System.err.println("Usage: Client <input s3 path> <output s3 path> <config file path s3> <aws access key> <aws secret key>");
			for (int i = 0; i < args.length; i++) {
				System.err.println(args[i]);
			}
			System.err.println(args.length);
			System.exit(-1);
		}
		
		String configFilePath = args[2];
		//Download file from S3
		BasicAWSCredentials awsCred = new BasicAWSCredentials(args[3], args[4]);
		S3Service awsAgent = new S3Service(awsCred);
		String configFileName = awsAgent.readOutputFromS3(configFilePath, awsCred);
		
		/*
		 * listen to /samples
		 */
		post(SAMPLE_FILE_URL,(req,res)->{
			res.status(200);
			res.body("SUCCESS RECEIVE SAMPLE");
			request_count++;
			LOG.log(Level.FINE, req.body().toString());
			samples.add(new Distribution(req.body().toString()));
			slaves.add(req.ip());
			if (request_count==SLAVE_NUM){
				postResult(samples);
			}
			
			return res.body().toString();
		});
	}

	private static String[] readConfigFileForInformation(String configFileName) throws IOException {
		List<String> ipList = new ArrayList<String>();
		FileReader fr = new FileReader(configFileName);
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] column = line.split(",");
			if (column[3].equals("S")) {
				ipList.add(column[2]);
			}
		}
		SLAVE_NUM = ipList.size();
		return (String[]) ipList.toArray();
	}

	/**
	 * post result to each slave
	 * @param samples
	 */
	public static void postResult(ArrayList<Distribution> samples){
		JSONObject obj = calculateDistribution(samples);
		String ret = obj.toJSONString();
		LOG.log(Level.INFO,"partitions: "+ret);
		for(String slaveIp:slaves){
			try {
				Unirest.post("http://" + slaveIp + ":"+DEFAULT_PORT+DISTRIBUTION_URL).body(ret).asString();
			} catch (UnirestException e) {
				LOG.log(Level.SEVERE, "UNABLE TO POST RESULT TO SLAVE: " + slaveIp);
			}
		}
	}
	
	/**
	 * calculate the distribution from all slaves
	 * @param dis
	 * @return
	 */
	public static JSONObject calculateDistribution(ArrayList<Distribution> dis) {
		ArrayList<Long> samples = new ArrayList<>();
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		for(Distribution distribution : dis){
			samples.addAll(distribution.getSamples());
			min = Math.min(min,distribution.getMin());
			max = Math.max(max,distribution.getMax());
		}
		return partition(samples,slaves,min,max);
	}
	
	/**
	 * partition the data to several job
	 * @param samples
	 * @param slaves
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject partition(ArrayList<Long> samples,ArrayList<String> slaves,long min,long max){
		Collections.sort(samples);
		int length = samples.size();
		JSONObject obj = new JSONObject();
		JSONArray arr = new JSONArray();
		int range = samples.size()/slaves.size();
		try{
			for(int i=0,node=0;i<length&&node<slaves.size();node++){
				JSONObject job = new JSONObject();
				int endpos = Math.min(length-1,i+range);
				long end = samples.get(endpos);
				
				if (node==0) job.put("min",min);
				else job.put("min", samples.get(i));
				
				if (node==slaves.size()-1) job.put("max",max);
				else job.put("max",end); 
				job.put("nodeIp",slaves.get(node));
				job.put("instanceId",node);
				arr.add(job);
				i+=range;
				while(i<length&&samples.get(i)==end) i++;
			}
		} catch (JSONException e){
			LOG.log(Level.SEVERE,e.getMessage());
			LOG.log(Level.SEVERE,"failed to genearte global distribution");
		}
		obj.put("partitions", arr);
		return obj;
	}
}
