package neu.edu.mr.client;

import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * 
 * @author yuanjianlai
 *
 */
public class Distribution {
	public static final String delimiter = ",";
	private final static Logger LOG = Logger.getLogger("Distribution");
	private static JSONParser parser = new JSONParser();
	private ArrayList<Double> samples;

	@SuppressWarnings("unchecked")
	public Distribution(File file) {
		if (!file.getName().endsWith(".dis.json")) {
			LOG.log(Level.INFO, "File not for distribution. file name: " + file.getName());
		}
		try {
			JSONObject obj = (JSONObject) parser.parse(new FileReader(file));
			JSONArray arr = (JSONArray) obj.get("samples");
			samples = new ArrayList<>(arr);
		} catch (Exception e) {
			LOG.log(Level.SEVERE, e.getMessage());
		}
	}

	public Distribution(String str) {
		try {
			String[] samplesArr = str.split(",");
			samples = new ArrayList<Double>();
			for (String num : samplesArr) {
				samples.add(Double.parseDouble(num));
			}
			LOG.info("Received samples " + samples.size());
		} catch (Exception e) {
			// TODO remove str
			LOG.log(Level.SEVERE, "Failed parsing sample : " + str + e.getMessage());
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			LOG.severe("Stacktrace: " + errors.toString());
		}
	}

	public ArrayList<Double> getSamples() {
		return samples;
	}

	public void setSamples(ArrayList<Double> samples) {
		this.samples = samples;
	}

}
