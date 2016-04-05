package edu.hadoop.a9.common;

import java.util.logging.Logger;

public class ClientNodeCommWrapper {
	public void SendData( String data ) {
		log.info(String.format("Sending data: %s", data));
	}
	
	private static final Logger log = Logger.getLogger(ClientNodeCommWrapper.class.getName());
}
