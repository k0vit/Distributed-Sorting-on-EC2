package edu.hadoop.a9.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
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
			String instanceIP) {
		this.file = f;
		this.ipToMaxMap = ipToMaxMap;
		this.ipToMinMap = ipToMinMap;
		this.INSTANCE_IP = instanceIP;
		this.ipToCountOfRequests = new HashMap<String, Integer>(ipToMaxMap.size());
		this.ipToActualRequestString = new HashMap<String, StringBuilder>(ipToMaxMap.size());
		File fil = new File(INSTANCE_IP + "-allhourly.csv");
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				log.info("Could not create a new file: " + f.getAbsolutePath());
			}
		}
		try {
			this.fw = new FileWriter(fil);
		} catch (IOException e) {
			log.severe("Could not create a new file: " + f.getAbsolutePath());
		}
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
								if (!f.exists()) {
									f.createNewFile();
									log.info("Trying to create a file named: " + f.getName());
								}
								
								FileWriter localfw = new FileWriter(f, true);
								localfw.write(Arrays.toString(line) + "\n");
								localfw.close();
								
								
							}
							break;
						}
					}
				}
			}
			fw.close();
			reader.close();
			SortNode.filesShuffledCount.getAndIncrement();
			log.info("No of files processed: " + SortNode.filesShuffledCount.get());
		} catch (Exception e) {
			log.severe("Failed while parsing value: " + e.getLocalizedMessage());
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			log.severe("Stacktrace: " + errors.toString());
		}
	}
}

