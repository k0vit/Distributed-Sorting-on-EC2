package Slave.SortNode;

import static spark.Spark.*;

/**
 * Test node 2 checking streaming data from one sort node to another.
 * @author naineel
 *
 */
public class sortNode2 {

	public static void main(String[] args) {
		port(1234);
		
		post("/records", (request, response) -> {
			byte[] bytes = request.bodyAsBytes();
			String record = new String(bytes);
			System.out.println(record);
			response.body("Great");
			response.status(200);
			return response.body().toString();
		});
	}

}
