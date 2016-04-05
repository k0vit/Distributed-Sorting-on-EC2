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
	private int max;
	private int min;
	private ArrayList<Integer> samples;
	
	public Distribution(File file){
		if (!file.getName().endsWith(".dis.json")){
			LOG.log(Level.INFO, "File not for distribution. file name: "+file.getName());
		}
		try{
			JSONObject obj = (JSONObject) parser.parse(new FileReader(file));
			max = (Integer) obj.get("max");
			min = (Integer) obj.get("min");
			String[] arr = ((String) obj.get("samples")).split(delimiter);
			samples = new ArrayList<>();
			for(String num:arr){
				samples.add(Integer.parseInt(num));
			}
		} catch (Exception e){
			LOG.log(Level.SEVERE, e.getMessage());
		}
	}
	
	public Distribution(String str){
		try{
			JSONObject obj = (JSONObject) parser.parse(str);
			max = (Integer) obj.get("max");
			min = (Integer) obj.get("min");
			JSONArray arr = (JSONArray) obj.get("samples");
			samples = new ArrayList<>();
			for(Object num:arr.toArray()){
				samples.add((Integer) num);
			}
		} catch (Exception e){
			LOG.log(Level.SEVERE, e.getMessage());
		}
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public ArrayList<Integer> getSamples() {
		return samples;
	}

	public void setSamples(ArrayList<Integer> samples) {
		this.samples = samples;
	}
	
}
