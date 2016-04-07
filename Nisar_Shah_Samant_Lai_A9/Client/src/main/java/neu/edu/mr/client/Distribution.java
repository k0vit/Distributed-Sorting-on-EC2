package neu.edu.mr.client;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Distribution {
	public static final String delimiter = ",";
	private final static Logger LOG = Logger.getLogger("Distribution");
	private static JSONParser parser = new JSONParser();
//	private long max;
//	private long min;
	private ArrayList<Long> samples;
	
	@SuppressWarnings("unchecked")
	public Distribution(File file){
		if (!file.getName().endsWith(".dis.json")){
			LOG.log(Level.INFO, "File not for distribution. file name: "+file.getName());
		}
		try{
			JSONObject obj = (JSONObject) parser.parse(new FileReader(file));
//			max = (Long) obj.get("max");
//			min = (Long) obj.get("min");
			JSONArray arr = (JSONArray) obj.get("samples");
			samples = new ArrayList<>(arr);
		} catch (Exception e){
			LOG.log(Level.SEVERE, e.getMessage());
		}
	}
	
	public Distribution(String str){
		try{
			JSONObject obj = (JSONObject) parser.parse(str);
//			max = (Long) obj.get("max");
//			min = (Long) obj.get("min");
			JSONArray arr = (JSONArray) obj.get("samples");
			samples = new ArrayList<>();
			for(Object num:arr.toArray()){
				samples.add((Long) num);
			}
		} catch (Exception e){
			LOG.log(Level.SEVERE, e.getMessage());
		}
	}


	public ArrayList<Long> getSamples() {
		return samples;
	}

	public void setSamples(ArrayList<Long> samples) {
		this.samples = samples;
	}
	
}
