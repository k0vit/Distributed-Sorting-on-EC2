package Slave.SortNode;

import java.util.Arrays;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Request {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnirestException {
		JSONObject obj1 = new JSONObject();
		obj1.put("min", new Integer(1));
		obj1.put("max", new Integer(50));
		obj1.put("nodeIp", "IP1");
		obj1.put("instanceId", 1);
		
		JSONObject obj2 = new JSONObject();
		obj2.put("min", new Integer(50));
		obj2.put("max", new Integer(60));
		obj2.put("nodeIp", "IP2");
		obj2.put("instanceId", 2);
		
		JSONObject obj3 = new JSONObject();
		obj3.put("min", new Integer(60));
		obj3.put("max", new Integer(100));
		obj3.put("nodeIp", "IP3");
		obj3.put("instanceId", 3);
		
		JSONArray array = new JSONArray();
		array.add(obj1);
		array.add(obj2);
		array.add(obj3);
		
		JSONObject mainObject = new JSONObject();
		mainObject.put("partitions", array);
//		System.out.println(mainObject.toJSONString());
		String body1 = mainObject.toJSONString();
//		System.out.println(mainObject.toJSONString());
		Unirest.setTimeouts(10000, 0);
		Unirest.post("http://localhost:4567/partitions").body(body1).asString();
		
		String body2 = obj2.toJSONString();
		System.out.println(obj2.toJSONString());
		Unirest.post("http://localhost:4567/partitions").body(body2).asString();
		
		String body3 = obj3.toJSONString();
		System.out.println(obj3.toJSONString());
		Unirest.post("http://localhost:4567/partitions").body(body3).asString();
//		
//		String[] string = new String[] {
//			"AS", "1", "100", "OAS10"	
//		};
//		String singleString = Arrays.toString(string);
//		byte[] bytes = singleString.getBytes();
//		Unirest.post("http://localhost:4567/records").body(bytes).asString();
	}

}
