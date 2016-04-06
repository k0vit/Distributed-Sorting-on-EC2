package neu.edu.mr.manager;

import java.io.File;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class ClusterStarter {
	
	private ClusterParams params;
	private AmazonS3Client client;
	
	public ClusterStarter(ClusterParams params) {
		client = new AmazonS3Client(new BasicAWSCredentials(params.getAccessKey(), params.getSecretKey()));
		client.setRegion(Region.getRegion(Regions.fromName("us-east-1")));
		this.params = params;
	}

	public boolean uploadToS3() {
		uploadFile(Main.CLUSTER_DETAILS_FILE_NAME);
		uploadFile(Main.CLIENT_JAR);
		uploadFile(Main.SORT_NODE_JAR);
		return false;
	}

	boolean uploadFile(String file) {
		File local = new File(file);
		if (!(local.exists() && local.canRead() && local.isFile())) {
			return false;
		}
		String folder = removeS3(params.getBucket());
		String bucket = folder;
		String remote = local.getName();
		client.putObject(new PutObjectRequest(bucket, remote, local));
		return true;
	}

	private static String removeS3(String path) {
		if (!path.startsWith("s3://"))
			return path;
		return path.substring("s3://".length());
	}
}
