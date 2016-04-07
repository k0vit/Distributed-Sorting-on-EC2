package Slave.SortNode;

import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.auth.BasicAWSCredentials;
import com.mashape.unirest.http.Unirest;
import com.opencsv.CSVReader;

public class AfterPartitionProgram {
	//TODO change to dynamic value
	//SORT NODE PROPERTIES
	static int TOTAL_NO_OF_SORT_NODES;
	static Map<String, Long> ipToMaxMap = new HashMap<String, Long>();
	static Map<String, Long> ipToMinMap = new HashMap<String, Long>();
	static long MINIMUM_PARTITION;
	static long MAXIMUM_PARTITION;
	static String INSTANCE_IP;
	static ArrayList<String[]> unsortedData = new ArrayList<String[]>();
	
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length != 5) {
			//CLass name will change after merging.
			System.err.println("Usage: SortNode <input s3 path> <output s3 path> <config file path s3> <aws access key> <aws secret key>");
			for (int i = 0; i < args.length; i++) {
				System.err.println(args[i]);
			}
			System.err.println(args.length);
			System.exit(-1);
		}
		
		INSTANCE_IP = InetAddress.getLocalHost().getHostName();
		
		String configFilePath = args[2];
		//Download file from S3
		BasicAWSCredentials awsCred = new BasicAWSCredentials(args[3], args[4]);
		S3Service awsAgent = new S3Service(awsCred);
		String configFileName = awsAgent.readOutputFromS3(configFilePath, awsCred);
		
		readFileAndSetProps(configFileName);
		
		JSONParser parser = new JSONParser();
		post("/partitions", (request, response) -> {
			// String partitions;
			response.status(200);
			response.body("SUCCESS");
			JSONObject entireJSON = (JSONObject) parser.parse(request.body().toString());
			System.out.println("This is: " + entireJSON.get("partitions"));
			JSONArray array = (JSONArray) entireJSON.get("partitions");
			for (int i = 0; i < array.size(); i++) {
				JSONObject jsonObject = (JSONObject) array.get(i);
				Long minimumPartition = (Long) jsonObject.get("min");
				Long maximumPartition = (Long) jsonObject.get("max");
				String nodeIp = (String) jsonObject.get("nodeIp");
				if (nodeIp == INSTANCE_IP) {
					MAXIMUM_PARTITION = maximumPartition;
					MINIMUM_PARTITION = minimumPartition;
				}
				ipToMaxMap.put(nodeIp, maximumPartition);
				ipToMinMap.put(nodeIp, minimumPartition);
			}
			System.out.println(request.body());
			// Read S3 data line by line
			File[] dataFolder = listDirectory("/home/naineel/a9Data/oneFolder/");
			for (File file : dataFolder) {
				FileInputStream fis = new FileInputStream(file);
				InputStream gzipStream = new GZIPInputStream(fis);
				BufferedReader br = new BufferedReader(new InputStreamReader(gzipStream));
				CSVReader reader = new CSVReader(br);
				String[] line = null;
				reader.readNext();
				while ((line = reader.readNext()) != null) {
					if (!(line.length < 9) && !line[8].equals("-")) {
						try {
							double dryBulbTemp = Double.parseDouble(line[8]);
							// Check which partition it lies within and
							// assign
							// to a mapElement in the entireData map
							for (String instanceIp : ipToMaxMap.keySet()) {
								if (dryBulbTemp >= ipToMinMap.get(instanceIp) && dryBulbTemp <= ipToMaxMap.get(instanceIp)) {
									if (instanceIp == INSTANCE_IP) {
										unsortedData.add(line);
									} else {
										//TODO add 100000 records and then send it as post.
										//TODO use StringBuilder as 20000 records colon separated.
										String record = Arrays.toString(line);
//										byte[] bytes = record.getBytes();
										Unirest.post("http://" + instanceIp + ":" + "1234" + "/records").body(record).asString();
									}
									break;
								}
							}
						} catch (Exception e) {}
					} 
				}
//				System.out.println("Lines read: " + reader.getLinesRead());
				reader.close();
			}
			sortYourOwnData();
			
			System.out.println("Finished now");
			return response.body().toString();
		});
		
		post("/records", (request, response) -> {
			//TODO colon separated list of records.
			String record = request.body();
			System.out.println(record);
			response.status(200);
			response.body("Awesome");
			return response.body().toString();
		});

		System.out.println("---End of program---");
		
		//TODO create one more post which will receive sort node message that they are done sorting data and then sortYourOwnData and 
		// use INSTANCE_ID to append to outputfile and then write to S3 output path.
	}

	private static void readFileAndSetProps(String configFileName) throws NumberFormatException, IOException {
		FileReader fr = new FileReader(configFileName);
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] column = line.split(",");
			if (column[3].equals("S")) {
				TOTAL_NO_OF_SORT_NODES++;
			}
		}
	}

	private static void sortYourOwnData() {
		Collections.sort(unsortedData, new Comparator<String[]>() {
			@Override
			public int compare(String[] o1, String[] o2) {
				return (o1[8].compareTo(o2[8]));
			}
		});
	}

	public static File[] listDirectory(String directoryPath) {
		File directory = new File(directoryPath);
		File[] files = directory.listFiles();
		return files;
	}
}
