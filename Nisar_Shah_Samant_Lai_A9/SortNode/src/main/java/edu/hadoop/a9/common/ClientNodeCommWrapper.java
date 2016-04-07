package edu.hadoop.a9.common;

import java.util.logging.Logger;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import edu.hadoop.a9.config.Configuration;

public class ClientNodeCommWrapper {
	public ClientNodeCommWrapper() {
		config = Configuration.getConfiguration();
	}
	
	public void SendData(String clientIp, String data) throws UnirestException {
		log.info(String.format("Sending data: %s", data));
		Unirest.post("http://" + clientIp + ":" + "4567").body(data).asString();
	}
	
	private final Configuration config;
	
	private static final Logger log = Logger.getLogger(ClientNodeCommWrapper.class.getName());
}
