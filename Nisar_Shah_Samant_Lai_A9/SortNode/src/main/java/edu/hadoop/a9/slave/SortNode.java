package edu.hadoop.a9.slave;

import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.opencsv.CSVReader;

import edu.hadoop.a9.common.NodeCommWrapper;
import edu.hadoop.a9.common.S3Wrapper;

public class SortNode {
	private static final Logger log = Logger.getLogger(SortNode.class.getName());
	static String accessKey;
	static String secretKey;
	static String clientIp;
	public static final int DRY_BULB_COL = 8;
	static int TOTAL_NO_OF_SORT_NODES;
	static Map<String, Long> ipToMaxMap = new HashMap<String, Long>();
	static Map<String, Long> ipToMinMap = new HashMap<String, Long>();
	static long MINIMUM_PARTITION;
	static long MAXIMUM_PARTITION;
	static String INSTANCE_IP;
	static long INSTANCE_ID;
	static ArrayList<String[]> unsortedData = new ArrayList<String[]>();
	// To avoid synchronization issues create one more list of records.
	static ArrayList<String[]> dataFromOtherNodes = new ArrayList<String[]>();
	public static final String PORT_FOR_COMM = "4567";
	public static final int NUMBER_OF_REQUESTS_STORED = 20000;
	public static final String PARTITION_URL = "partitions";
	public static final String END_URL = "end";
	public static final String END_OF_SORTING_URL = "signals";
	static int NO_OF_SORT_NODES_WHERE_DATA_IS_RECEIVED = 0;

	public static void main(String[] args) {
		if (args.length != 5) {
			System.err.println(
					"Usage: SortNode <input s3 path> <output s3 path> <config file path s3> <aws access key> <aws secret key>");
			for (int i = 0; i < args.length; i++) {
				System.err.println(args[i]);
			}
			System.err.println(args.length);
			System.exit(-1);
		}

		log.info(String.format("<input s3 path>: %s <output s3 path>: %s <config file path s3>: %s", args[0], args[1], args[2]));
		
		String inputS3Path = args[0];
		String outputS3Path = args[1];
		String configFilePath = args[2];
		accessKey = args[3];
		secretKey = args[4];

		log.info("Application Initialized");

		try {
			INSTANCE_IP = InetAddress.getLocalHost().getHostName();
			log.info("Instance IP: " + INSTANCE_IP);
			BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
			AmazonS3Client s3client = new AmazonS3Client(awsCredentials);
			S3Wrapper wrapper = new S3Wrapper(s3client);

			// This downloads the config file on the local directory.
			String configFileName = wrapper.readOutputFromS3(configFilePath, awsCredentials);
			log.info("Config file name: " + configFileName);
			readFileAndSetProps(configFileName);

			// This is the first thing node will do as soon as it is up.
			sendSampleDistribution(wrapper, awsCredentials, inputS3Path);

			// Read partitions from client and send data to other sort nodes.
			readPartitionsFromClient();

			// Receive data from other sort nodes in the different list.
			receiveDataFromOtherSortNodes();

			// Once all data is received then sort the data and upload result
			// file to S3.
			checkIfAllDataReceived(outputS3Path, wrapper);

		} catch (IOException e) {
			log.severe(e.getMessage());
		}

	}

	/**
	 * 
	 */
	private static void receiveDataFromOtherSortNodes() {

		post("/records", (request, response) -> {
			String recordList = request.body();
			String[] records = recordList.split(":");
			log.info(String.format("Received %s records from [%s]", records.length, request.ip()));
			for (String record : records) {
				dataFromOtherNodes.add(record.split(","));
			}
			response.status(200);
			response.body("Awesome");
			return response.body().toString();
		});

	}

	/**
	 * If All data is received then start sorting the data you have and write it to S3.
	 * 
	 */
	private static void checkIfAllDataReceived(String outputS3Path, S3Wrapper wrapper) {
		post("/end", (request, response) -> {
			NO_OF_SORT_NODES_WHERE_DATA_IS_RECEIVED++;
			if (NO_OF_SORT_NODES_WHERE_DATA_IS_RECEIVED == TOTAL_NO_OF_SORT_NODES - 1) {
				log.info("Received data from all sort nodes");
				log.info("Start sorting data....");
				sortYourOwnData();
				if (wrapper.uploadDataToS3(outputS3Path, unsortedData, INSTANCE_ID)) {
					log.info(String.format("Data uploaded to S3 @ %s", outputS3Path));
					NodeCommWrapper.SendData(clientIp, PORT_FOR_COMM, END_OF_SORTING_URL, "SORTED");
				}
			}
			return response.body().toString();
		});

	}

	/**
	 * 
	 */
	public static void readPartitionsFromClient() {
		JSONParser parser = new JSONParser();
		Map<String, Integer> ipToCountOfRequests = new HashMap<String, Integer>();
		Map<String, StringBuilder> ipToActualRequestString = new HashMap<String, StringBuilder>();

		post("/partitions", (request, response) -> {
			log.info("Received partitions from the client!");
			response.status(200);
			response.body("SUCCESS");
			JSONObject entireJSON = (JSONObject) parser.parse(request.body().toString());
			JSONArray array = (JSONArray) entireJSON.get("partitions");
			for (int i = 0; i < array.size(); i++) {
				JSONObject jsonObject = (JSONObject) array.get(i);
				Long minimumPartition = (Long) jsonObject.get("min");
				Long maximumPartition = (Long) jsonObject.get("max");
				String nodeIp = (String) jsonObject.get("nodeIp");
				String instanceId = (String) jsonObject.get("instanceId");
				if (instanceId.equals("NOWORK")) {
					NodeCommWrapper.SendData(nodeIp, PORT_FOR_COMM, END_URL, "EOF");
					log.info("Sent EOF as no partition is assigned to the sort node");
					return response.body().toString();
				} else {
					Long instanceIdLong = Long.parseLong(instanceId);
					if (nodeIp == INSTANCE_IP) {
						MAXIMUM_PARTITION = maximumPartition;
						MINIMUM_PARTITION = minimumPartition;
						INSTANCE_ID = instanceIdLong;
						log.info(String.format("Sort Node Info: InstanceId: %s maxPartition: %s minPartition: %s", INSTANCE_ID, MAXIMUM_PARTITION, MINIMUM_PARTITION));
					}
					ipToMaxMap.put(nodeIp, maximumPartition);
					ipToMinMap.put(nodeIp, minimumPartition);
				}
			}

			// Read local data line by line
			File[] dataFolder = listDirectory(System.getProperty("user.dir"));
			for (File file : dataFolder) {
				if (!checkFileExtensionsIsGz(file.getName()))
					continue;
				FileInputStream fis = new FileInputStream(file);
				InputStream gzipStream = new GZIPInputStream(fis);
				BufferedReader br = new BufferedReader(new InputStreamReader(gzipStream));
				CSVReader reader = new CSVReader(br);
				String[] line = null;
				reader.readNext();
				while ((line = reader.readNext()) != null) {
					if (!(line.length < 9) && !line[DRY_BULB_COL].equals("-")) {
						double dryBulbTemp = Double.parseDouble(line[DRY_BULB_COL]);
						// Check which partition it lies within and send to
						// the sortNode required
						for (String instanceIp : ipToMaxMap.keySet()) {
							if (dryBulbTemp >= ipToMinMap.get(instanceIp)
									&& dryBulbTemp <= ipToMaxMap.get(instanceIp)) {
								if (instanceIp == INSTANCE_IP) {
									unsortedData.add(line);
								} else {
									if (ipToCountOfRequests.get(instanceIp) < NUMBER_OF_REQUESTS_STORED) {
										ipToCountOfRequests.put(instanceIp, ipToCountOfRequests.get(instanceIp) + 1);
										ipToActualRequestString.put(instanceIp,
												ipToActualRequestString.get(instanceIp).append(":" + line));
									} else {
										sendRequestToSortNode(instanceIp, ipToCountOfRequests, ipToActualRequestString);
									}
								}
								break;
							}
						}
					}
				}
				reader.close();
				br.close();

				for (String ipAddress : ipToCountOfRequests.keySet()) {
					log.info("Flush out remaining data to Sort Node: " + ipAddress);
					sendRequestToSortNode(ipAddress, ipToCountOfRequests, ipToActualRequestString);
					// SEND EOF to signal end of file
					NodeCommWrapper.SendData(ipAddress, PORT_FOR_COMM, END_URL, "EOF");
				}

			}

			return response.body().toString();
		});

	}

	/**
	 * 
	 * @param instanceIp
	 * @param ipToCountOfRequests
	 * @param ipToActualRequestString
	 */
	private static void sendRequestToSortNode(String instanceIp, Map<String, Integer> ipToCountOfRequests,
			Map<String, StringBuilder> ipToActualRequestString) {
		StringBuilder sb = ipToActualRequestString.get(instanceIp);
		ipToActualRequestString.put(instanceIp, new StringBuilder());
		ipToCountOfRequests.put(instanceIp, 0);
		String recordList = sb.toString();
		NodeCommWrapper.SendData(instanceIp, PORT_FOR_COMM, PARTITION_URL, recordList);
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

	private static void readFileAndSetProps(String configFileName) {
		FileReader fr;
		try {
			fr = new FileReader(configFileName);

			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] column = line.split(",");
				if (column[3].equals("S")) {
					TOTAL_NO_OF_SORT_NODES++;
				}
			}
			log.info("Total number of sort nodes: " + TOTAL_NO_OF_SORT_NODES);
			br.close();
		} catch (IOException e) {
			log.severe("File reader error: " + e.getMessage());
		}
	}

	public static void sendSampleDistribution(S3Wrapper wrapper, BasicAWSCredentials awsCredentials,
			String inputS3Path) {
		try {
			post("/files", (request, response) -> {
				// Receive request from client for the files which need to be
				// taken care of
				clientIp = request.ip();
				log.info("Received files to be handled from Client IP: " + clientIp);
				response.status(200);
				response.body("SUCCESS");
				String fileString = request.body();
				String[] filenames = fileString.split(",");
				randomlySample(filenames, awsCredentials, inputS3Path);
				return response.body().toString();
			});

		} catch (Exception exp) {
			StringWriter sw = new StringWriter();
			exp.printStackTrace(new PrintWriter(sw));
			log.severe(String.format("Error sending sampled distribution : %s", exp.getMessage()));
			log.severe(sw.toString());
		}
	}

	/**
	 * This method takes the string of filenames, creates separate threads and
	 * sends each file sampling to client Node.
	 * 
	 * @param filenames
	 */
	private static void randomlySample(String[] filenames, BasicAWSCredentials awsCredentials, String inputS3Path) {
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		for (String filename : filenames) {
			//Check if filename is ending with .gz
			if (checkFileExtensionsIsGz(filename)) {
				Task task = new Task(filename, clientIp, awsCredentials, inputS3Path);
				log.info("Start task");
				executor.execute(task);
			} else {
				log.info(String.format("Filename: %s does not end with .gz, Thus skipped", filename));
			}
		}
		log.info("Executor shutdown");
		executor.shutdown();
	}

	/**
	 * 
	 */
	private static void sortYourOwnData() {
		log.info("Sorting sort node data now...");
		unsortedData.addAll(dataFromOtherNodes);
		Collections.sort(unsortedData, new Comparator<String[]>() {
			@Override
			public int compare(String[] o1, String[] o2) {
				return (o1[8].compareTo(o2[8]));
			}
		});
		log.info("Sorting sort node data finished !");
	}

	/**
	 * 
	 * @param directoryPath
	 * @return
	 */
	public static File[] listDirectory(String directoryPath) {
		log.info("Listing folder: " + directoryPath);
		File directory = new File(directoryPath);
		File[] files = directory.listFiles();
		return files;
	}

}
