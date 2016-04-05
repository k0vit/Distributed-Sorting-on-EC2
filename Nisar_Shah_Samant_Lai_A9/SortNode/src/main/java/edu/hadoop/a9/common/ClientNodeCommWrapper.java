package edu.hadoop.a9.common;

import java.util.logging.Logger;

import com.mashape.unirest.http.Unirest;

import edu.hadoop.a9.config.Configuration;

public class ClientNodeCommWrapper {
	public ClientNodeCommWrapper() {
		config = Configuration.getConfiguration();
	}
	
	public void SendData( String data ) {
		log.info(String.format("Sending data: %s", data));
		boolean sendToClient = Boolean.parseBoolean(config.getProperty("client.send"));
		if( sendToClient )
			Unirest.post(config.getProperty("client.url")).body(data);
	}
	
	private final Configuration config;
	
	private static final Logger log = Logger.getLogger(ClientNodeCommWrapper.class.getName());
}
