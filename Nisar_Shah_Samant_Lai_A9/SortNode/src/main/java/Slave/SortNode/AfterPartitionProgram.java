package Slave.SortNode;

import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
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

import com.mashape.unirest.http.Unirest;
import com.opencsv.CSVReader;

public class AfterPartitionProgram {
	//TODO change to dynamic value
	final static int TOTAL_NO_OF_NODES = 3;
	static Map<Long, Long> maxMap = new HashMap<Long, Long>();
	static Map<Long, Long> minMap = new HashMap<Long, Long>();
	static long MINIMUM;
	static long MAXIMUM;
	final static Long INSTANCE_ID = 1L;
	static ArrayList<String[]> unsortedData = new ArrayList<String[]>();
	
	public static void main(String[] args) {
		JSONParser parser = new JSONParser();
		post("/partitions", (request, response) -> {
			// String partitions;
			response.status(200);
			response.body("SUCCESS: " + INSTANCE_ID);
			JSONObject entireJSON = (JSONObject) parser.parse(request.body().toString());
			System.out.println("THis is: " + entireJSON.get("partitions"));
			JSONArray array = (JSONArray) entireJSON.get("partitions");
			for (int i = 0; i < array.size(); i++) {
				JSONObject jsonObject = (JSONObject) array.get(i);
				Long minimumPartition = (Long) jsonObject.get("min");
				Long maximumPartition = (Long) jsonObject.get("max");
				Long instanceId = (Long) jsonObject.get("instanceId");
				if (instanceId == INSTANCE_ID) {
					MAXIMUM = maximumPartition;
					MINIMUM = minimumPartition;
				}
				maxMap.put(instanceId, maximumPartition);
				minMap.put(instanceId, minimumPartition);
			}
			System.out.println(request.body());
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
								if (dryBulbTemp > minMap.get(instanceNo) && dryBulbTemp <= maxMap.get(instanceNo)) {
									if (instanceNo == INSTANCE_ID) {
										unsortedData.add(line);
									} else {
										String record = Arrays.toString(line);
										byte[] bytes = record.getBytes();
										Unirest.post("http://localhost:1234/records").body(bytes).asString();
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
			byte[] bytes = request.bodyAsBytes();
			String record = new String(bytes);
			System.out.println(record);
			response.status(200);
			response.body("Awesome");
			return response.body().toString();
		});

		System.out.println("Done- hashmaps populated");
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
