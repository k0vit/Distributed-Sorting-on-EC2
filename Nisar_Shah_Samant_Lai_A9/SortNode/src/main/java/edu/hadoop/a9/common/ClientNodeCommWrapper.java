package edu.hadoop.a9.common;

import java.util.logging.Logger;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class ClientNodeCommWrapper {
	
	public static void SendData(String clientIp, String port, String requestUrl, String data) throws UnirestException {
		log.info(String.format("Sending data: %s", data));
		Unirest.post("http://" + clientIp + ":" + port + "/" + requestUrl).body(data).asString();
	}
	
	private static final Logger log = Logger.getLogger(ClientNodeCommWrapper.class.getName());
}
