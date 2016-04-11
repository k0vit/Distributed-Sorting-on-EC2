package neu.edu.mr.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * action = create, terminate or start
 * @author kovit
 *
 */
public class Main {

	public static final String keyName = "a9key";
	public static final String securityGrpName = "a9securitygrp";
	public static final String CLUSTER_MANAGER_LOGGER = "ClusterManager";
	private final static Logger LOGGER = Logger.getLogger(CLUSTER_MANAGER_LOGGER);
	public static final String CLUSTER_DETAILS_FILE_NAME = "InstanceDetails.csv";
	public static final String EC2_KEY_FILE_NAME = "ec2key.pem";
	public static final String CLIENT_JAR = "client-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
	public static final String SORT_NODE_JAR = "SortNode-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
	public static Logger logger; 

	/**
	 * 0 -action 
	 * 1 - number of nodes or sort column name or s3 output path based on action
	 * 2 - s3 input location
	 * 3 - s3 output location
	 * @param args
	 */
	public static void main(String[] args) {
		logger = logSetup();

		ClusterParams params = new ClusterParams("cluster.properties");

		LOGGER.log(Level.FINE, "Action to be performed " + args[0]);
		if (args[0].equalsIgnoreCase("create")) {
			ClusterCreator creator = new ClusterCreator(params);
			boolean clusterCreated = creator.createCluster();
			LOGGER.log(Level.FINE, "Cluster created successfully: " + clusterCreated);
		}
		else if (args[0].equalsIgnoreCase("terminate")) {
			ClusterTerminator terminator = new ClusterTerminator(params);
			boolean clusterTerminated = terminator.terminateCluster();
			LOGGER.log(Level.FINE, "Cluster terminated successfully: " + clusterTerminated);
			boolean downloadOutput = terminator.downloadOutput(args[1]);
			LOGGER.log(Level.FINE, "downloaded output successfully: " + downloadOutput);
		}
		else if (args[0].equalsIgnoreCase("start")) {
			ClusterStarter starter = new ClusterStarter(params);
			boolean uploadFile = starter.uploadToS3();
			LOGGER.log(Level.FINE, "File uploaded successfully: " + uploadFile);
		}
		else {
			System.err.println("Incorrect action " + args[0] + ". Action can either be [create] or [terminate]");
			System.exit(-1);
		}
	}

	private static Logger logSetup() {
		Logger logger = Logger.getLogger(CLUSTER_MANAGER_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		logger.setLevel(Level.ALL);
		logger.addHandler(handler);
		return logger;
	}
}

class ClusterParams {
	Properties clusterProp = new Properties();

	public ClusterParams(String fileName) {
		readProperties(fileName);
	}

	public String getAccessKey() {
		return clusterProp.getProperty("AccessKey");
	}
	public String getSecretKey() {
		return clusterProp.getProperty("SecretKey");
	}
	public String getBaseImageName() {
		return clusterProp.getProperty("BaseImage");
	}
	public String getInstanceFlavor() {
		return clusterProp.getProperty("InstanceType");
	}
	public int getNoOfInstance() {
		return Integer.parseInt(clusterProp.getProperty("InstanceNumber"));
	}
	public String getBucket() {
		return clusterProp.getProperty("Bucket");
	}

	private void readProperties(String fileName) {
		InputStream input = null;
		try {
			input = new FileInputStream(System.getProperty("user.dir") + File.separator + fileName);
			clusterProp.load(input);
		} catch (Exception ex) {
			System.err.println("Failed while reading properties from " + fileName);
			System.exit(-1);
		} 
	}
}
