package neu.edu.mr.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * 
 * @author yuanjianlai
 *
 */
public class ClientTest extends TestCase{
	
	ArrayList<Long> sortedsample;
	ArrayList<String> slaves;
	protected void setUp(){
		sortedsample = new ArrayList<>();
		slaves = new ArrayList<>();
	}
	
	public ClientTest(String testName) {
        super( testName );
    }
	
	public static Test suite(){
		return new TestSuite(ClientTest.class);
	}
	
	/*
	 * test the partitioning logic
	 */
	public void testPartitioning(){
		for(int i=1;i<=3;i++){
			slaves.add(Integer.toString(i));
		}
		for(int i=1;i<=100;i++){
			sortedsample.add((long) i);
		}
		JSONObject obj = Client.partition(sortedsample,slaves);
		System.out.println(obj.toJSONString());
	}
	
	/*
	 * test reading config from the config file
	 */
	public void testReadingConfig() throws IOException{
		InputStream input = new FileInputStream(new File("src/test/java/neu/edu/mr/client/test_config"));
		Client.readConfigInfo(input);
		assertEquals(3, Client.SLAVE_NUM);
		assertEquals(3, Client.slaves.size());
		assertEquals("172.31.49.249",Client.slaves.get(0));
		assertEquals("172.31.49.250",Client.slaves.get(1));
		assertEquals("172.31.49.251",Client.slaves.get(2));
	}
	/*
	 * test job split method
	 */
	public void testDistributeJob(){
		int fileNum = 5;
		int slaveNum = 3;
		Client.FILE_NUM = fileNum;
		Client.SLAVE_NUM = slaveNum;
		
		List<String> fileList = new ArrayList<>();
		for(int i=0;i<fileNum;i++){
			fileList.add("file_"+i);
		}
		List<String> shares = Client.divideJobs(fileList);
		assertEquals(Client.SLAVE_NUM, shares.size());
		assertEquals(shares.get(0).split(Client.DELIMITER_OF_FILE).length, 2);
		assertEquals(shares.get(1).split(Client.DELIMITER_OF_FILE).length, 2);
		assertEquals(shares.get(2).split(Client.DELIMITER_OF_FILE).length, 1);
		
		fileList.clear();
		fileNum = 4;
		slaveNum = 3;
		Client.FILE_NUM = fileNum;
		Client.SLAVE_NUM = slaveNum;
		for(int i=0;i<fileNum;i++){
			fileList.add("file_"+i);
		}
		shares = Client.divideJobs(fileList);
		assertEquals(Client.SLAVE_NUM, shares.size());
		assertEquals(shares.get(0).split(Client.DELIMITER_OF_FILE).length, 2);
		assertEquals(shares.get(1).split(Client.DELIMITER_OF_FILE).length, 1);
		assertEquals(shares.get(2).split(Client.DELIMITER_OF_FILE).length, 1);
		
		fileList.clear();
		fileNum = 6;
		slaveNum = 3;
		Client.FILE_NUM = fileNum;
		Client.SLAVE_NUM = slaveNum;
		for(int i=0;i<fileNum;i++){
			fileList.add("file_"+i);
		}
		shares = Client.divideJobs(fileList);
		assertEquals(Client.SLAVE_NUM, shares.size());
		assertEquals(shares.get(0).split(Client.DELIMITER_OF_FILE).length, 2);
		assertEquals(shares.get(1).split(Client.DELIMITER_OF_FILE).length, 2);
		assertEquals(shares.get(2).split(Client.DELIMITER_OF_FILE).length, 2);
	}
	
	
	
}
