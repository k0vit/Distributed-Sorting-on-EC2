package neu.edu.mr.client;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class MessageSender {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnirestException{
		JSONObject obj1 = new JSONObject();
		JSONArray arr1 = new JSONArray();
		for(int i=1;i<=100;i++){
			arr1.add(i);
		}
		obj1.put("min",-1);
		obj1.put("max",300);
		obj1.put("samples",arr1);
		Unirest.post("http://localhost:4567/samples").body(obj1.toJSONString()).asString();
		
		JSONObject obj2 = new JSONObject();
		JSONArray arr2 = new JSONArray();
		for(int i=50;i<=150;i++){
			arr2.add(i);
		}
		obj2.put("min",-2);
		obj2.put("max",300);
		obj2.put("samples",arr2);
		Unirest.post("http://localhost:4567/samples").body(obj2.toJSONString()).asString();
		
		JSONObject obj3 = new JSONObject();
		JSONArray arr3 = new JSONArray();
		for(int i=100;i<=200;i++){
			arr3.add(i);
		}
		obj3.put("min",-1);
		obj3.put("max",400);
		obj3.put("samples",arr3);
		Unirest.post("http://localhost:4567/samples").body(obj3.toJSONString()).asString();
		
	}
}
