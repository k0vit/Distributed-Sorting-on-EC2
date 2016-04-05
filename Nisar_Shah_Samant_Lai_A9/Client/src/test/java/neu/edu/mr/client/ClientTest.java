package neu.edu.mr.client;

import java.util.ArrayList;

import org.json.simple.JSONObject;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ClientTest extends TestCase{
	
	ArrayList<Integer> sortedsample;
	ArrayList<String> slaves;
	protected void setUp(){
		sortedsample = new ArrayList<>();
		slaves = new ArrayList<>();
	}
	
	public ClientTest(String testName) {
        super( testName );
    }
	
	public Test suite(){
		return new TestSuite(ClientTest.class);
	}
	
	public void testClient(){
		for(int i=1;i<=3;i++){
			slaves.add(Integer.toString(i));
		}
		for(int i=1;i<=100;i++){
			sortedsample.add(i);
		}
		JSONObject obj = Client.partition(sortedsample,slaves,-1,200);
		System.out.println(obj.toJSONString());
		assertTrue(true);
	}
}
