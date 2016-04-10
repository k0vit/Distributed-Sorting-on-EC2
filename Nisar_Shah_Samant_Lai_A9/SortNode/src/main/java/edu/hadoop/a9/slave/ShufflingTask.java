package edu.hadoop.a9.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.opencsv.CSVReader;

public class ShufflingTask implements Runnable {

	private File file;
	private final Logger log = Logger.getLogger(SortNode.class.getName());
	private Map<String, Double> ipToMaxMap;
	private Map<String, Double> ipToMinMap;
	private String INSTANCE_IP;
	private List<String[]> unsortedData;
	Map<String, Integer> ipToCountOfRequests;
	Map<String, StringBuilder> ipToActualRequestString;
	public ShufflingTask(File f, Map<String, Double> ipToMaxMap, Map<String, Double> ipToMinMap, String instanceIP) {
		this.file = f;
		this.ipToMaxMap = ipToMaxMap;
		this.ipToMinMap = ipToMinMap;
		this.INSTANCE_IP = instanceIP;
		this.ipToCountOfRequests = new HashMap<String, Integer>(ipToMaxMap.size());
		this.ipToActualRequestString = new HashMap<String, StringBuilder>(ipToMaxMap.size());
		unsortedData = new LinkedList<>();
		
	}

	public void run() {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))){
			CSVReader reader = new CSVReader(br);
			String[] line = null;
			reader.readNext();
			while ((line = reader.readNext()) != null) {
				if (!(line.length < 9) && !line[SortNode.DRY_BULB_COL].equals("-")) {
					double dryBulbTemp;
					try {
						dryBulbTemp = Double.parseDouble(line[SortNode.DRY_BULB_COL]);
					} catch (Exception e) {
						log.info("Failed to parse value to Double: " + line[SortNode.DRY_BULB_COL]);
						continue;
					}
					// Check which partition it lies within and send to
					// the sortNode required
					for (String instanceIp : ipToMaxMap.keySet()) {
						if (dryBulbTemp >= ipToMinMap.get(instanceIp)
								&& dryBulbTemp <= ipToMaxMap.get(instanceIp)) {
							if (instanceIp.equals(INSTANCE_IP)) {
								unsortedData.add(line);
							} else {
								if (!ipToCountOfRequests.containsKey(instanceIp)) {
									ipToCountOfRequests.put(instanceIp, 0);
								}
								if (!ipToActualRequestString.containsKey(instanceIp)) {
									ipToActualRequestString.put(instanceIp, new StringBuilder());
								}
								
								if (ipToCountOfRequests.get(instanceIp) < SortNode.NUMBER_OF_REQUESTS_STORED) {
									ipToCountOfRequests.put(instanceIp, ipToCountOfRequests.get(instanceIp) + 1);
									ipToActualRequestString.put(instanceIp,
											ipToActualRequestString.get(instanceIp).append(Arrays.toString(line) + ":"));
								} else {
									ipToActualRequestString.put(instanceIp,
											ipToActualRequestString.get(instanceIp).append(Arrays.toString(line) + ":"));
									SortNode.sendRequestToSortNode(instanceIp, ipToCountOfRequests, ipToActualRequestString);
								}
							}
							break;
						}
					}
				}
			}
			reader.close();
			SortNode.filesShuffledCount.getAndIncrement();
			log.info("No of files processed: " + SortNode.filesShuffledCount.get());
			SortNode.addUnsortedData(unsortedData);
			
			for (String ipAddress : ipToCountOfRequests.keySet()) {
				log.info("Flush out remaining data to Sort Node: " + ipAddress);
				if (ipToCountOfRequests.get(ipAddress) > 0) {
					SortNode.sendRequestToSortNode(ipAddress, ipToCountOfRequests, ipToActualRequestString);
				}
			}
		} catch (Exception e) {
			log.severe("Failed while parsing value: " + e.getLocalizedMessage());
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			log.severe("Stacktrace: " + errors.toString());
		}
	}
}