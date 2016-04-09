package neu.edu.mr.client;

import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import neu.edu.mr.utility.S3Service;

/**
 * @author yuanjianlai
 *
 */
public class Client {
	private final static Logger LOG = Logger.getLogger("Client");
	private final static String SAMPLE_FILE_URL = "/samples";
	private final static String DISTRIBUTION_URL = "/partitions";
	private final static String SIGNAL_URL = "/signals";
	private final static String FILES_URL = "/files";
	private final static String DEFAULT_PORT = "4567";
	public final static String NULL_JOB_SIGNAL = "NOWORK";
	public final static String DELIMITER_OF_FILE = ",";
	private static AtomicInteger request_count = new AtomicInteger(0);
	private static int signal_count = 0;
	public static int SLAVE_NUM;
	public static int FILE_NUM;
	public static long startTime;
	private static List<Distribution> samples = Collections.synchronizedList(new ArrayList<Distribution>());
	protected static ArrayList<String> slaves;
	private static S3Service s3;
	private static int SLAVE_WITH_NO_WORK = 0;

	public static void main(String[] args) {
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

		/*
		 * initialize aws cred and S3 component
		 */
		BasicAWSCredentials awsCred = new BasicAWSCredentials(args[3], args[4]);
		s3 = new S3Service(awsCred);

		// read config files
		LOG.log(Level.INFO, "reading configuration");
		try {
			readConfigInfo(args[2]);
		} catch (IOException e) {
			LOG.log(Level.SEVERE, "failed to read configuration file: " + e.getMessage());
		}

		// send file names to each sort node. /files post request
		LOG.log(Level.INFO, "distributing jobs");
		distributeJob(args[0]);

		/*
		 * listen to /samples to get samples
		 */
		post(SAMPLE_FILE_URL, (req, res) -> {
			res.status(200);
			res.body("SUCCESS RECEIVE SAMPLE");
			request_count.getAndIncrement();
			LOG.info("Received request from " + req.ip() + " on port " + req.port());
			LOG.info("total request from files " + request_count.get());
			LOG.info("SAMPLES RECEIVED: " + req.body().toString());
			samples.add(new Distribution(req.body().toString()));
			if (request_count.get() == FILE_NUM) {
				LOG.info("request count = to file number. Posting partitions");
				postResult(samples);
			}

			return res.body().toString();
		});

		// get signal from sort node that its done and to and calculate timing.
		post(SIGNAL_URL, (req, res) -> {
			res.status(200);
			res.body("SUCCESS SIGNAL RECEIVED");
			signal_count++;
			LOG.log(Level.INFO, "Sort work done signal from: " + req.ip());
			LOG.info("total signal count " + signal_count);
			if (signal_count == SLAVE_NUM - SLAVE_WITH_NO_WORK) {
				long totalTime = System.currentTimeMillis() - startTime;
				System.out.println("Sort Job Done");
				System.out.println("Total Time: " + totalTime / 1000 + " Seconds");
				System.exit(0);
			}
			return res.body().toString();
		});
	}

	/**
	 * read the input folder and distribute the job to sort nodes
	 * 
	 * @param inputS3Path
	 */
	protected static void distributeJob(String inputS3Path) {
		List<String> files = s3.getListOfObjects(inputS3Path);
		LOG.info("Actual files count from s3 " + files.size());
		List<String> curatedFiles = new ArrayList<String>();
		for (String file : files) {
			if (checkFileExtensionsIsGz(file)) {
				String onlyFileName = file.split("/")[1];
				curatedFiles.add(onlyFileName);
			}
		}

		LOG.info("Listing s3 objects " + curatedFiles);
		FILE_NUM = curatedFiles.size();
		LOG.info("file size " + FILE_NUM);
		List<String> shares = divideJobs(curatedFiles);
		for (int i = 0; i < SLAVE_NUM; i++) {
			String slaveIp = slaves.get(i);
			String filesShare = shares.get(i);
			String req = "http://" + slaveIp + ":" + DEFAULT_PORT + FILES_URL;
			try {
				Unirest.post(req).body(filesShare).asString();
				LOG.info("Posting to " + req + "with body as " + filesShare);
			} catch (Exception e) {
				LOG.log(Level.SEVERE, "Failed to distribute files to slave " + req + ". Reason " + e.getMessage());
				LOG.severe("Sleeping for 10 seconds to retry");
				try {
					Thread.sleep(10000);
				} 
				catch (Exception ex) {
					LOG.severe("Failed to sleep " + ex.getMessage());
				}
				try {
					LOG.severe("Retrying " + req);
					Unirest.post(req).body(filesShare).asString();
				} 
				catch (Exception exx) {
					LOG.severe("Failed to retry to post " + req);
				}
			}
		}
	}

	/**
	 * divide the work by sort nodes
	 * 
	 * @param files
	 * @return
	 */
	protected static List<String> divideJobs(List<String> files) {
		List<String> shares = new ArrayList<String>();
		int share = FILE_NUM / SLAVE_NUM;
		int mod = FILE_NUM % SLAVE_NUM;
		LOG.info("Share count " + share);
		LOG.info("Mod count " + mod);
		for (int j = 0, i = 0; j < SLAVE_NUM; j++) {
			StringBuilder sb = new StringBuilder();
			for (int k = i; i - k < share && i < FILE_NUM; i++) {
				sb.append(DELIMITER_OF_FILE);
				sb.append(files.get(i));
			}
			if (j < mod) {
				sb.append(DELIMITER_OF_FILE);
				sb.append(files.get(i++));
			}
			LOG.info("Share for node " + j + " is " + i);
			sb.deleteCharAt(0);
			shares.add(sb.toString());
		}
		return shares;
	}

	/**
	 * read config file
	 * 
	 * @param configFilePath
	 * @param accessKey
	 * @param secretKey
	 * @throws IOException
	 */
	protected static void readConfigInfo(String configFilePath) throws IOException {
		readConfigInfo(s3.getObjectInputStream(configFilePath));
	}

	/**
	 * read config file and get slaves addr
	 * 
	 * @return a list of the slaves addr
	 * @throws IOException
	 */
	protected static void readConfigInfo(InputStream input) throws IOException {
		slaves = new ArrayList<String>();
		try (InputStream fileInputStream = input) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] column = line.split(",");
				if (column[3].equals("S")) {
					slaves.add(column[1]);
				}
			}
			LOG.info("slaves " + slaves);
			SLAVE_NUM = slaves.size();
		}
	}

	/**
	 * post result to each slave
	 * 
	 * @param samples
	 */
	public static void postResult(List<Distribution> samples) {
		JSONObject obj = calculateDistribution(samples);
		String ret = obj.toJSONString();
		LOG.log(Level.INFO, "partitions: " + ret);
		for (String slaveIp : slaves) {
			try {
				String req = "http://" + slaveIp + ":" + DEFAULT_PORT + DISTRIBUTION_URL;
				LOG.info("posting request to " + req + " with body as " + ret);
				Unirest.post(req).body(ret).asString();
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
	public static JSONObject calculateDistribution(List<Distribution> dis) {
		ArrayList<Double> samples = new ArrayList<>();
		for (Distribution distribution : dis) {
			samples.addAll(distribution.getSamples());
		}
		LOG.info("Total number of samples " + samples.size());
		return partition(samples, slaves);
	}

	/**
	 * partition the data to several jobs
	 * 
	 * @param samples
	 * @param slaves
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject partition(ArrayList<Double> samples, ArrayList<String> slaves) {
		Collections.sort(samples);
		int length = samples.size();
		JSONObject obj = new JSONObject();
		JSONArray arr = new JSONArray();
		int range = samples.size() / slaves.size();
		try {
			int node = 0;
			double pre = Double.MIN_VALUE;
			for (int i = 0; i < length && node < slaves.size(); node++) {
				JSONObject job = new JSONObject();
				int endpos = Math.min(length - 1, i + range - 1);
				double end = samples.get(endpos);

				if (node == 0)
					job.put("min", Double.MIN_VALUE);
				else
					job.put("min", pre + 0.1);

				if (node == slaves.size() - 1 || i + range >= length)
					job.put("max", Double.MAX_VALUE);
				else
					job.put("max", end);

				job.put("nodeIp", slaves.get(node));
				job.put("instanceId", node + "");
				arr.add(job);
				i += range;
				pre = end;
				while (i < length && samples.get(i) == end)
					i++;
			}
			/*
			 * for those nodes that dont have to work, send a dummy signal
			 */
			while (node < slaves.size()) {
				JSONObject nullJob = new JSONObject();
				nullJob.put("min", 0);
				nullJob.put("max", 0);
				nullJob.put("nodeIp", slaves.get(node));
				nullJob.put("instanceId", NULL_JOB_SIGNAL);
				arr.add(nullJob);
				node++;
				SLAVE_WITH_NO_WORK++;
			}
			LOG.info("There are " + SLAVE_WITH_NO_WORK + " slaves with no work");
		} catch (JSONException e) {
			LOG.log(Level.SEVERE, e.getMessage());
			LOG.log(Level.SEVERE, "failed to generate global distribution");
		}
		obj.put("partitions", arr);
		return obj;
	}

	/**
	 * Check file extension is .gz or not.
	 * 
	 * @param fileName
	 * @return
	 */
	private static boolean checkFileExtensionsIsGz(String fileName) {
		String format = fileName.substring(fileName.lastIndexOf(".") + 1);
		if (format.equals("gz")) {
			return true;
		} else {
			return false;
		}
	}
}
