package Slave.SortNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.opencsv.CSVReader;

import static spark.Spark.post;

public class AfterPartitionProgram {
	final static int MAX_REQUESTS_TO_SORT_NODE = 3;
	static int noOfRequestsToSortNode = 0;
	static Map<Long, Long> maxMap = new HashMap<Long, Long>();
	static Map<Long, Long> minMap = new HashMap<Long, Long>();
	static long MINIMUM;
	static long MAXIMUM;
	static Map<Long, ArrayList<String[]>> entireDataMap = new HashMap<Long, ArrayList<String[]>>();

	public static void main(String[] args) {
		final String INSTANCE_ID = "1";

		JSONParser parser = new JSONParser();
		post("/partitions", (request, response) -> {
			// String partitions;
			response.status(200);
			response.body("SUCCESS: " + INSTANCE_ID);
			JSONObject entireJSON = (JSONObject) parser.parse(request.body().toString());
			System.out.println("THis is: " + entireJSON.get("instance-id"));
			Long minimumPartition = (Long) entireJSON.get("min");
			Long maximumPartition = (Long) entireJSON.get("max");
			Long instanceId = (Long) entireJSON.get("instance-id");
			if (instanceId == Long.parseLong(INSTANCE_ID)) {
				MAXIMUM = maximumPartition;
				MINIMUM = minimumPartition;
			}
			maxMap.put(instanceId, maximumPartition);
			minMap.put(instanceId, minimumPartition);
			// partitions = request.body();
			System.out.println(request.body());
			noOfRequestsToSortNode++;
			if (noOfRequestsToSortNode == MAX_REQUESTS_TO_SORT_NODE) {
				// Read S3 data line by line and create a map
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
								for (Long instanceNo : maxMap.keySet()) {
									if (dryBulbTemp > minMap.get(instanceNo) && dryBulbTemp < maxMap.get(instanceNo)) {
										if (entireDataMap.get(instanceNo) != null) {
											ArrayList<String[]> strings = entireDataMap.get(instanceNo);
											strings.add(line);
											entireDataMap.put(instanceNo, strings);
										} else {
											ArrayList<String[]> newList = new ArrayList<String[]>();
											newList.add(line);
											entireDataMap.put(instanceNo, newList);
										}
									}
								}
							} catch (Exception e) {
								System.out.println("Line" + line + "has an error: " + e);
							}
						} else if (line.length < 9){
							System.out.println("Line is " + line);
						}
					}
					System.out.println("Lines read: " + reader.getLinesRead());
					reader.close();
				}

				FileOutputStream fos = new FileOutputStream("/home/naineel/hashmap.ser");
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(entireDataMap);
				oos.close();
				fos.close();
				System.out.println("Finished now");
			}

			return response.body().toString();
		});

		System.out.println("Done- hasmaps populated");
	}

	public static File[] listDirectory(String directoryPath) {
		File directory = new File(directoryPath);
		File[] files = directory.listFiles();
		return files;
	}

}
