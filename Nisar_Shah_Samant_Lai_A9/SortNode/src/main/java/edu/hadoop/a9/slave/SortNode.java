package edu.hadoop.a9.slave;

import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.multi.DownloadPackage;
import org.jets3t.service.multi.SimpleThreadedStorageService;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import edu.hadoop.a9.common.NodeCommWrapper;
import edu.hadoop.a9.common.S3Wrapper;

public class SortNode {
	private static final Logger log = Logger.getLogger(SortNode.class.getName());
	static String accessKey;
	static String secretKey;
	static String clientIp;
	public static final int DRY_BULB_COL = 8;
	static int TOTAL_NO_OF_SORT_NODES;
	static Map<String, Double> ipToMaxMap;
	static Map<String, Double> ipToMinMap;
	static Double MINIMUM_PARTITION;
	static Double MAXIMUM_PARTITION;
	static String INSTANCE_IP;
	static long INSTANCE_ID;
	public static final String PORT_FOR_COMM = "4567";
	public static final int NUMBER_OF_REQUESTS_STORED = 90000;
	public static final String PARTITION_URL = "partitions";
	public static final String END_URL = "end";
	public static final String END_OF_SORTING_URL = "signals";
	public static final String RECORDS_URL = "records";
	static AtomicInteger NO_OF_SORT_NODES_WHERE_DATA_IS_RECEIVED = new AtomicInteger(0);
	static String jsonPartitions;
	static boolean partitionReceived = false;
	static int NO_OF_NODES_WITH_WORK = 0;
	public static AtomicInteger filesShuffledCount = new AtomicInteger(0);
	public static List<String> s3FileLocation = Collections.synchronizedList(new LinkedList<String>());
//	public static List<String[]> unsortedData = Collections.synchronizedList(new LinkedList<String[]>());
	public static S3Wrapper wrapper;
	public static String outputS3Path;
	private static FileWriter fw;

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

		log.info(String.format("<input s3 path>: %s <output s3 path>: %s <config file path s3>: %s", args[0], args[1],
				args[2]));

		String inputS3Path = args[0];
		outputS3Path = args[1];
		String configFilePath = args[2];
		accessKey = args[3];
		secretKey = args[4];
		String configFileName = configFilePath.substring(configFilePath.lastIndexOf("/") + 1);

		// Load your default settings from jets3t.properties file on the classpath
		Jets3tProperties myProperties =
				Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);

		// Override default properties (increase number of connections and
//		myProperties.setProperty("httpclient.max-connections", "100");
		myProperties.setProperty("threaded-service.max-thread-count", "8");
		myProperties.setProperty("threaded-service.admin-max-thread-count", "8");
		myProperties.setProperty("s3service.max-thread-count", "8");
		
		log.info("Application Initialized");

		try {
			ClientConfiguration conf = new ClientConfiguration();
			conf.setMaxConnections(1000);
			INSTANCE_IP = InetAddress.getLocalHost().getHostAddress();
			log.info("Instance IP: " + INSTANCE_IP);
			BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
			AmazonS3Client s3client = new AmazonS3Client(awsCredentials, conf);
			wrapper = new S3Wrapper(s3client);

			// This downloads the config file on the local directory.
			configFileName = wrapper.readOutputFromS3(configFilePath, awsCredentials, configFileName);
			log.info("Config file name: " + configFileName);
			readFileAndSetProps(configFileName);

			log.info("Entering method: sendSampleDistribution");
			// This is the first thing node will do as soon as it is up.
			sendSampleDistribution(wrapper, awsCredentials, inputS3Path);
			log.info("Leaving method: sendSampleDistribution");

			// Receive data from other sort nodes in the different list.
			log.info("Entering method: receiveDataFromOtherSortNodes");
//			receiveDataFromOtherSortNodes();
			log.info("Leaving method: receiveDataFromOtherSortNodes");

			// Once all data is received then sort the data and upload result
			// file to S3.
			log.info("Entering method: checkIfAllDataReceived");
			checkIfAllDataReceived(outputS3Path, wrapper, awsCredentials);
			log.info("Leaving method: checkIfAllDataReceived");

			log.info("Entering method: readPartitionsFromClient");
			// Read partitions from client and send data to other sort nodes.
			readPartitionsFromClient();
			log.info("Leaving method: readPartitionsFromClient");

			log.info("Entering method: sendDataToOtherSortNodes");
			sendDataToOtherSortNodes();
			log.info("Leaving method: sendDataToOtherSortNodes");

		} catch (IOException e) {
			log.severe(e.getMessage());
		}
	}

	/**
	 * 
	 */
	private static void sendDataToOtherSortNodes() {
		JSONParser parser = new JSONParser();

		JSONObject entireJSON = null;
		try {
			entireJSON = (JSONObject) parser.parse(jsonPartitions);
		} catch (ParseException e1) {
			log.severe("Failed while parsing partition: " + jsonPartitions);
			StringWriter errors = new StringWriter();
			e1.printStackTrace(new PrintWriter(errors));
			log.severe("Stacktrace: " + errors.toString());
		}
		JSONArray array = (JSONArray) entireJSON.get("partitions");
		for (int i = 0; i < array.size(); i++) {
			JSONObject jsonObject = (JSONObject) array.get(i);
			Double minimumPartition = (Double) jsonObject.get("min");
			Double maximumPartition = (Double) jsonObject.get("max");
			String nodeIp = (String) jsonObject.get("nodeIp");
			String instanceId = (String) jsonObject.get("instanceId");
			if (instanceId.equals("NOWORK")) {
				if (nodeIp == INSTANCE_IP) {
					System.exit(0);
				} else {
					log.info("nodeIp: " + nodeIp + " with NOWORK");
				}
			} else {
				log.info("Comparing nodeIp: " + nodeIp + " INSTANCE_IP: " + INSTANCE_IP);
				if (nodeIp.equals(INSTANCE_IP)) {
					MAXIMUM_PARTITION = maximumPartition;
					MINIMUM_PARTITION = minimumPartition;
					INSTANCE_ID = Long.valueOf(instanceId);
					log.info(String.format("Sort Node Info: InstanceId: %s maxPartition: %s minPartition: %s",
							INSTANCE_ID, MAXIMUM_PARTITION, MINIMUM_PARTITION));
				}
				NO_OF_NODES_WITH_WORK++;
				ipToMaxMap.put(nodeIp, maximumPartition);
				ipToMinMap.put(nodeIp, minimumPartition);
			}
		}

		// Read local data line by line
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);
		File[] dataFolder = listDirectory(System.getProperty("user.dir"));
		int totalFiles = 0;
		for (File file : dataFolder) {
			if (checkFileExtensionsIsGz(file.getName())) {
				totalFiles++;
				ShufflingTask task = new ShufflingTask(file, ipToMaxMap, ipToMinMap, INSTANCE_IP);
				log.info("ShufflingTask started");
				executor.execute(task);
			}
		}
		log.info("Executor shutdown");
		executor.shutdown();

		/*try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			log.info("Executor interrupted");
		}*/

		while (filesShuffledCount.get() != totalFiles) {
			log.info("Waiting for all files to be reshuffled");
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				log.severe("Thread sleep interrupted when waiting for all files to be reshuffled");
			}
		}
		
		log.info("Reshuffling done");
		
		for (String ipAddress : ipToMaxMap.keySet()) {
			//Upload file to S3
			try {
				File[] files = listDirectory(System.getProperty("user.dir") + "/" + ipAddress);
				List objectsToUploadAsMultipart = new ArrayList();
				for (File singleFile : files) {
					S3Object largeObj = new S3Object(singleFile);
					objectsToUploadAsMultipart.add(largeObj);
				}
				
				long maxSizeForAPartInBytes = 20 * 1024 * 1024;
				MultipartUtils mpUtils = new MultipartUtils(maxSizeForAPartInBytes);
				
				AWSCredentials awsCred = new AWSCredentials(accessKey, secretKey);
				
				S3Service s3Service = new RestS3Service(awsCred);
//				String bucketPath = outputS3Path.replace("s3://", "");
				s3Service.createBucket(ipAddress);
				log.info("THE BUCKET PATH IS " + ipAddress);
				mpUtils.uploadObjects(ipAddress, s3Service, objectsToUploadAsMultipart, null);
				for (File singleFile: files) {
					if(singleFile.delete()) {
						log.info("File deleted successfully: " + singleFile.getAbsolutePath());
					}
				}
			} catch (Exception e) {
				log.info("JetS3t error: " + e.getMessage());
			}
			NodeCommWrapper.SendData(ipAddress, PORT_FOR_COMM, END_URL, "EOF");
		}
	}
	
	public static synchronized void addUnsortedData(List<String[]> unsortedDat) {
		File f = new File(INSTANCE_IP + "-allhourly.csv");
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				log.info("Could not create a new file: " + f.getAbsolutePath());
			}
		}
		try {
			fw = new FileWriter(f);
		} catch (IOException e) {
			log.info("Could not open file for writing: " + f.getAbsolutePath());
		}
		for (String[] st : unsortedDat) {
			try {
				fw.write(Arrays.toString(st));
			} catch (IOException e) {
				log.info("Could not write to file: " + f.getAbsolutePath());
			}
		}
		
//		unsortedData.addAll(unsortedDat);
	}

	/**
	 * 
	 */
	private static void receiveDataFromOtherSortNodes() {

		post("/records", (request, response) -> {
			String recordList = request.body();
			String[] records = recordList.split(",");
			log.info(String.format("Received %s records from [%s]", records, request.ip()));
			for (String record : records) {
				s3FileLocation.add(record);
			}
			records = null;
			log.info("Data files from other nodes has size: " + s3FileLocation.size());
			response.status(200);
			response.body("Awesome");
			return response.body().toString();
		});

	}

	/**
	 * If All data is received then start sorting the data you have and write it
	 * to S3.
	 * 
	 */
	private static void checkIfAllDataReceived(String outputS3Path, S3Wrapper wrapper, BasicAWSCredentials awsCredentials) {
		post("/end", (request, response) -> {
			NO_OF_SORT_NODES_WHERE_DATA_IS_RECEIVED.getAndIncrement();
			if (NO_OF_SORT_NODES_WHERE_DATA_IS_RECEIVED.get() == NO_OF_NODES_WITH_WORK) {
				log.info("Received data from all sort nodes");
				log.info("Start sorting data....");
//				log.info("Files to sort from " + s3FileLocation);
				//DOwnload data from S3Bucket into local file.
				AWSCredentials awsCred = new AWSCredentials(accessKey, secretKey);
				S3Service s3Service = new RestS3Service(awsCred);
				S3Bucket s3Bucket = s3Service.getBucket(INSTANCE_IP);
				S3Object[] bucketFiles = s3Service.listObjects(s3Bucket.getName());
				SimpleThreadedStorageService simpleMulti = new SimpleThreadedStorageService(s3Service);
				DownloadPackage[] downloadPackages = new DownloadPackage[bucketFiles.length];
				for (int i = 0; i < downloadPackages.length; i++) {
					downloadPackages[i] = new DownloadPackage(bucketFiles[i], new File(bucketFiles[i].getKey()));
				}	
				
				simpleMulti.downloadObjects(INSTANCE_IP, downloadPackages);
				
				sortYourOwnData();
				//if (wrapper.uploadFileS3(outputS3Path, unsortedData, INSTANCE_ID)) {
				
				
				//UPLOAD TO S3 LOGIC
				String outputPath = outputS3Path.replace("s3://", "");
				String[] splitOutput = outputPath.split("/"); 
				String bucketName = splitOutput[0];
				String key = splitOutput[1];
				
				File resultFile = new File(System.getProperty("user.dir") + "/" + "part-r-" + INSTANCE_ID + ".csv");
				List objectsToUploadAsMultipart = new ArrayList();
				S3Object largeObj = new S3Object(resultFile);
				largeObj.setKey(key + "/" + largeObj.getKey());
				objectsToUploadAsMultipart.add(largeObj);
				
				long maxSizeForAPartInBytes = 20 * 1024 * 1024;
				MultipartUtils mpUtils = new MultipartUtils(maxSizeForAPartInBytes);
				
				log.info("THE BUCKET PATH IS " + outputS3Path);
				mpUtils.uploadObjects(bucketName, s3Service, objectsToUploadAsMultipart, null);
				
				log.info("This is INSTANCE_ID: " + INSTANCE_ID);
				log.info(String.format("Data uploaded to S3 @ %s", outputS3Path));
				
				NodeCommWrapper.SendData(clientIp, PORT_FOR_COMM, END_OF_SORTING_URL, "SORTED");
			}
			response.status(200);
			response.body("SUCCESS");
			return response.body().toString();
		});

	}

	/**
	 * 
	 */
	public static void readPartitionsFromClient() {
		
		post("/partitions", (request, response) -> {
			log.info("Received partitions from the client!");
			log.info("Partition is as follows: " + request.body());
			response.status(200);
			response.body("SUCCESS");
			jsonPartitions = request.body();
			partitionReceived = true;
			return response.body().toString();
		});

		while (!partitionReceived) {
			try {
				Thread.sleep(20000);
				log.info("...");
			} catch (InterruptedException e) {
				log.info("Thread interrupted");
			}
		}
		log.info("Out of while loop");
	}
	/**
	 * 
	 * @param instanceIp
	 * @param ipToCountOfRequests
	 * @param ipToActualRequestString
	 */

	public static void sendRequestToSortNode(String recordList, String fileName, String instanceIp) {
		wrapper.uploadStringData(recordList, outputS3Path + "/" + instanceIp + "/" + fileName);
		NodeCommWrapper.SendData(instanceIp, PORT_FOR_COMM, RECORDS_URL, fileName);

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
			log.info(String.format("Reading config file from path: %s",
					System.getProperty("user.dir") + File.separator + configFileName));
			fr = new FileReader(System.getProperty("user.dir") + File.separator + configFileName);

			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] column = line.split(",");
				if (column[3].equals("S")) {
					TOTAL_NO_OF_SORT_NODES++;
				}
			}
			log.info("Total number of sort nodes: " + TOTAL_NO_OF_SORT_NODES);
			ipToMaxMap = new HashMap<String, Double>(TOTAL_NO_OF_SORT_NODES);
			ipToMinMap = new HashMap<String, Double>(TOTAL_NO_OF_SORT_NODES);
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
				log.info("Received files to be handled from Client IP: " + clientIp + " Port: " + request.port());
				log.info("Received files: " + request.body());
				response.status(200);
				response.body("SUCCESS");
				String fileString = request.body();
				String[] filenames = fileString.split(",");
				log.info("Received No of Files: " + filenames.length + " for sampling");
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
			// Check if filename is ending with .gz
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
		Path outFile = Paths.get(System.getProperty("user.dir"), "result.csv");
//		File[] files = listDirectory(System.getProperty("user.dir"));
		File root = new File(System.getProperty("user.dir"));
		File[] fileList = root.listFiles(new FilenameFilter() {
			public boolean accept(File root, String name) {
		        return name.matches(".*?hourly.csv.*");
		    }
		});
		
		//MERGING FILES LOGIC		
		try (FileChannel out = FileChannel.open(outFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
			for (int i = 0; i < fileList.length; i++) {
				Path inFile = fileList[i].toPath();
				log.info("Merging: " + inFile + "...");
				try (FileChannel in = FileChannel.open(inFile, StandardOpenOption.READ)) {
					for (long p = 0, l = in.size(); p < l; p++)
						p += in.transferTo(p, l - p, out);
				}
			}
			log.info("MERGING SUCCESSFUL");
		} catch (IOException e) {
			log.info("Exception in merging operation: " + e.getMessage());
		}
		
		log.info("Now starting external sort");
		try {
			ExternalSorter.externalSort("result.csv", "part-r-" + INSTANCE_ID + ".csv");
		} catch (IOException e) {
			log.info("External sort unsuccessful: " + e.getMessage());
		}
		
		
		
		
//		unsortedData.addAll(dataFromOtherNodes);
//		Collections.sort(unsortedData, new Comparator<String[]>() {
//			@Override
//			public int compare(String[] o1, String[] o2) {
//				if (o1.length < 9 || o2.length < 9) {
//					log.info("String length o1: " + o1.length);
//					log.info("String length o2: " + o2.length);
//					log.severe("o1: " + Arrays.toString(o1));
//					log.severe("o2: " + Arrays.toString(o2));
//					return 0;
//				} else {
//					return (o1[DRY_BULB_COL].compareTo(o2[DRY_BULB_COL]));
//				}
//			}
//		});
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

