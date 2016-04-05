package neu.edu.mr.client;
import static spark.Spark.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Client {
	private final static Logger LOG = Logger.getLogger("Client");
	private final static String SAMPLE_FILE_URL = "/samples";
	private final static String DISTRIBUTION_URL = "/partitions";
	private static int request_count = 0;
	public final static int SLAVE_NUM = 3;
	private static ArrayList<Distribution> samples = new ArrayList<>();
	private static ArrayList<String> slaves = new ArrayList<>();
	
	public static void main(String[] args){
		/*
		 * listen to /samples
		 */
		post(SAMPLE_FILE_URL,(req,res)->{
			res.status(200);
			res.body("SUCCESS RECEIVE SAMPLE");
			request_count++;
			samples.add(new Distribution(res.body().toString()));
			slaves.add(req.ip());
			if (request_count==SLAVE_NUM){
				postResult(samples);
			}
			return res.body().toString();
		});
	}
	
	
	/**
	 * post result to each slave
	 * @param samples
	 */
	public static void postResult(ArrayList<Distribution> samples){
		JSONObject obj = calculateDistribution(samples);
		String ret = obj.toJSONString();
		for(String slaveIp:slaves){
			try {
				Unirest.post("http://" + slaveIp + DISTRIBUTION_URL).body(ret).asString();
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
		ArrayList<Integer> samples = new ArrayList<>();
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
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
	public static JSONObject partition(ArrayList<Integer> samples,ArrayList<String> slaves,int min,int max){
		Collections.sort(samples);
		System.out.println("partitioning");
		int length = samples.size();
		JSONObject obj = new JSONObject();
		JSONArray arr = new JSONArray();
		int range = samples.size()/slaves.size();
		try{
			for(int i=0,node=0;i<length&&node<slaves.size();node++){
				JSONObject job = new JSONObject();
				int endpos = Math.min(length-1,i+range);
				int end = samples.get(endpos);
				
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
