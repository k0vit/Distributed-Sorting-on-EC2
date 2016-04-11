package edu.hadoop.a9.slave;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.code.externalsorting.ExternalSort;

/**
 * This will external sort the file in local.
 * 
 * @author yuanjian.
 *
 */
public class ExternalSorter {

	private final static Logger LOGGER = Logger.getLogger("External Sorter");

	public static void externalSort(String file, String output) throws IOException {
		List<File> l = ExternalSort.sortInBatch(new File(file), new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				try {
					return o1.split(",")[8].compareTo(o2.split(",")[8]);
				} catch (Exception e) {
					LOGGER.log(Level.WARNING, "corrupted line found: str1:" + o1 + "\n str2:" + o2);
					return 1;
				}
			}
		});
		ExternalSort.mergeSortedFiles(l, new File(output));
	}
}