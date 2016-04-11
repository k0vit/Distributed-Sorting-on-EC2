package edu.hadoop.a9.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
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
	Map<String, Integer> ipToCountOfRequests;
	Map<String, StringBuilder> ipToActualRequestString;
	int count;
	private FileWriter fw;

	public ShufflingTask(File f, Map<String, Double> ipToMaxMap, Map<String, Double> ipToMinMap,
			String instanceIP, FileWriter fw) {
		this.file = f;
		this.ipToMaxMap = ipToMaxMap;
		this.ipToMinMap = ipToMinMap;
		this.INSTANCE_IP = instanceIP;
		this.ipToCountOfRequests = new HashMap<String, Integer>(ipToMaxMap.size());
		this.ipToActualRequestString = new HashMap<String, StringBuilder>(ipToMaxMap.size());
		this.fw = fw;
	}

	public void run() {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))){
			CSVReader reader = new CSVReader(br);
			String[] line = null;
			reader.readNext();
			log.info("Going through file: " + file.getName());
			while ((line = reader.readNext()) != null) {
				if (!(line.length < 9) && !line[SortNode.DRY_BULB_COL].equals("-")) {
					double dryBulbTemp;
					try {
						dryBulbTemp = Double.parseDouble(line[SortNode.DRY_BULB_COL]);
					} catch (Exception e) {
						log.info(String.format("[%s] Failed to parse value to Double: %s", file.getName(), line[SortNode.DRY_BULB_COL]));
						continue;
					}
					// Check which partition it lies within and send to
					// the sortNode required
					for (String instanceIp : ipToMaxMap.keySet()) {
						if (dryBulbTemp >= ipToMinMap.get(instanceIp)
								&& dryBulbTemp <= ipToMaxMap.get(instanceIp)) {
							if (instanceIp.equals(INSTANCE_IP)) {
								fw.write(Arrays.toString(line));
							} else {
								String fileNameWithoutFormat = file.getName().replace(".txt.gz", "");
								File directory = new File(instanceIp);
								
								if (!directory.isDirectory()) {
									directory.mkdir();
									log.info("Creating directory: " + directory.getName());
								}
								
								File f = new File(instanceIp, instanceIp+ "-" + fileNameWithoutFormat + ".csv");
//								
								if (!f.exists()) {
									f.createNewFile();
									log.info("Trying to create a file named: " + f.getName());
								}
								
								fw = new FileWriter(f, true);
								fw.write(Arrays.toString(line) + "\n");
								fw.close();
//								if (!ipToCountOfRequests.containsKey(instanceIp)) {
//									ipToCountOfRequests.put(instanceIp, 0);
//								}
//								if (!ipToActualRequestString.containsKey(instanceIp)) {
//									ipToActualRequestString.put(instanceIp, new StringBuilder());
//								}
//
//								if (ipToCountOfRequests.get(instanceIp) < SortNode.NUMBER_OF_REQUESTS_STORED) {
//									ipToCountOfRequests.put(instanceIp, ipToCountOfRequests.get(instanceIp) + 1);
//									ipToActualRequestString.put(instanceIp,
//											ipToActualRequestString.get(instanceIp).append(Arrays.toString(line) + ":"));
//								} else {
//									ipToActualRequestString.put(instanceIp,
//											ipToActualRequestString.get(instanceIp).append(Arrays.toString(line) + ":"));
//									count++;
//									StringBuilder sb = ipToActualRequestString.get(instanceIp);
//									ipToActualRequestString.put(instanceIp, new StringBuilder());
//									ipToCountOfRequests.put(instanceIp, 0);
//									String recordList = sb.deleteCharAt(sb.length()-1).toString();
//									SortNode.sendRequestToSortNode(recordList, instanceIp + file.getName() + count,
//											instanceIp);
//								}
								
								
							}
							break;
						}
					}
				}
			}
			reader.close();
			SortNode.filesShuffledCount.getAndIncrement();
			log.info("No of files processed: " + SortNode.filesShuffledCount.get());

//		for (String ipAddress : ipToCountOfRequests.keySet()) {
//				log.info("Flush out remaining data to Sort Node: " + ipAddress);
//				if (ipToCountOfRequests.get(ipAddress) > 0) {
//					StringBuilder sb = ipToActualRequestString.get(ipAddress);
//					ipToActualRequestString.put(ipAddress, new StringBuilder());
//					ipToCountOfRequests.put(ipAddress, 0);
//					String recordList = sb.deleteCharAt(sb.length()-1).toString();
//					SortNode.sendRequestToSortNode(recordList, ipAddress + file.getName() + (++count), ipAddress);
//				}
//			}
		} catch (Exception e) {
			log.severe("Failed while parsing value: " + e.getLocalizedMessage());
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			log.severe("Stacktrace: " + errors.toString());
		}
	}
}

