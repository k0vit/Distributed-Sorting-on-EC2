package neu.edu.mr.manager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class ClusterTerminator {

	private AmazonEC2Client amazonEC2Client;
	private final static Logger LOGGER = Logger.getLogger(Main.CLUSTER_MANAGER_LOGGER);

	public ClusterTerminator(ClusterParams params) {
		amazonEC2Client = new AmazonEC2Client(new BasicAWSCredentials(params.getAccessKey(), params.getSecretKey()));
	}

	public boolean terminateCluster() {

		List<String> instanceIds = getTerminateInstanceIds();
		TerminateInstancesRequest req = new TerminateInstancesRequest(instanceIds);
		LOGGER.log(Level.FINE, "terminating instances " + instanceIds);
		amazonEC2Client.terminateInstances(req);
		amazonEC2Client.shutdown();
		return false;
	}

	private List<String> getTerminateInstanceIds() {
		List<String> instanceIds = new ArrayList<>();
		try {
			FileReader fileReader = new FileReader(Main.CLUSTER_DETAILS_FILE_NAME);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = null;
			while((line = bufferedReader.readLine()) != null) {
				instanceIds.add(line.split(",")[0]);
			}   
			bufferedReader.close();         
		}
		catch(Exception ex) {
			System.err.println("Failed to read file " + Main.CLUSTER_DETAILS_FILE_NAME);
			System.exit(-1);
		}
		return instanceIds;
	}
}
