package Slave.SortNode;

import org.json.simple.JSONObject;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Request {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnirestException {
		JSONObject obj1 = new JSONObject();
		obj1.put("min", new Integer(1));
		obj1.put("max", new Integer(30));
		obj1.put("nodeIp", "IP1");
		obj1.put("instance-id", 1);
		
		JSONObject obj2 = new JSONObject();
		obj2.put("min", new Integer(30));
		obj2.put("max", new Integer(60));
		obj2.put("nodeIp", "IP2");
		obj2.put("instance-id", 2);
		
		JSONObject obj3 = new JSONObject();
		obj3.put("min", new Integer(60));
		obj3.put("max", new Integer(100));
		obj3.put("nodeIp", "IP3");
		obj3.put("instance-id", 3);
		
//		JSONArray array = new JSONArray();
//		array.put(obj1);
//		array.put(obj2);
//		array.put(obj3);
		
//		JSONObject mainObject = new JSONObject();
//		mainObject.put("partitions", array);
		String body1 = obj1.toJSONString();
		System.out.println(obj1.toJSONString());
		Unirest.setTimeouts(10000, 0);
		Unirest.post("http://localhost:4567/partitions").body(body1).asString();
		
		String body2 = obj2.toJSONString();
		System.out.println(obj2.toJSONString());
		Unirest.post("http://localhost:4567/partitions").body(body2).asString();
		
		String body3 = obj3.toJSONString();
		System.out.println(obj3.toJSONString());
		Unirest.post("http://localhost:4567/partitions").body(body3).asString();
//		
		
	}

}
