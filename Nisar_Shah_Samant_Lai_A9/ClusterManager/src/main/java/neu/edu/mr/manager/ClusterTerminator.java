package neu.edu.mr.manager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class ClusterTerminator {

	private AmazonEC2Client amazonEC2Client;
	private final static Logger LOGGER = Logger.getLogger(Main.CLUSTER_MANAGER_LOGGER);

	public ClusterTerminator(ClusterParams params) {
		amazonEC2Client = new AmazonEC2Client(new BasicAWSCredentials(params.getAccessKey(), params.getSecretKey()));
	}

	public boolean terminateCluster() {
		List<String> instanceIds = getTerminateInstanceIds();
		try {
			TerminateInstancesRequest req = new TerminateInstancesRequest(instanceIds);
			amazonEC2Client.terminateInstances(req);
			LOGGER.log(Level.FINE, "terminating instances " + instanceIds);
			for (String id : instanceIds) {
				DescribeInstancesRequest statusReq = new DescribeInstancesRequest();
				statusReq.withInstanceIds(id);
				String state = InstanceStateName.Running.toString();
				while (!state.equals(InstanceStateName.Terminated.toString())) {
					Thread.sleep(10000);
					LOGGER.log(Level.FINE, "Sleeping for 10 seconds. State not changed");
					DescribeInstancesResult result = amazonEC2Client.describeInstances(statusReq);
					if (result.getReservations().size() > 0) {
						Instance inst = result.getReservations().get(0).getInstances().get(0);
						state = inst.getState().getName();
						LOGGER.log(Level.FINE, "State changed to " + state);
					}
				}
			}
			amazonEC2Client.shutdown();
			return true;
		}
		catch (Exception e) {
			System.err.println("Failed to terminate instances " + instanceIds);
			return false;
		}
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
