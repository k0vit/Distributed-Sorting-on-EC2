package edu.hadoop.a9.common;

import java.util.logging.Logger;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * 
 * @author diptiSamant
 *
 */
public class NodeCommWrapper {
	
	public static void SendData(String clientIp, String port, String requestUrl, String data) {
		String address = "http://" + clientIp + ":" + port + "/" + requestUrl;
		log.info(String.format("Sending data to %s", address));
		try {
			Unirest.post(address).body(data).asString();
		} catch (UnirestException e) {
			log.severe("Exception sending post request: " + e.getMessage());
		}
	}
	
	private static final Logger log = Logger.getLogger(NodeCommWrapper.class.getName());
}
