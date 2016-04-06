package neu.edu.mr.utility;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

public class S3Service {
	AmazonS3 s3client;

	public S3Service(BasicAWSCredentials cred) {
		super();
		this.s3client = new AmazonS3Client(cred);
	}

	public List<String> getListOfObjects(String bucketName, String prefix) throws AmazonServiceException {
		ListObjectsRequest request = new ListObjectsRequest();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);
		ArrayList<String> files = new ArrayList<String>();
		ObjectListing listing = null;
		do {
			listing = s3client.listObjects(request);
			for (S3ObjectSummary summary : listing.getObjectSummaries()) {
				files.add(summary.getKey());
			}
		} while (listing.isTruncated());

		return files;
	}

	public InputStream getObjectInputStream(String bucketName, String objectId) {
		GetObjectRequest request = new GetObjectRequest(bucketName, objectId);
		S3Object object = s3client.getObject(request);
		return object.getObjectContent();
	}
	
	public String readOutputFromS3(String outputPath, BasicAWSCredentials cred) throws IOException, InterruptedException {
		TransferManager tx = new TransferManager(cred);
		String simplifiedPath = (outputPath.replace("s3://", ""));
		int index = simplifiedPath.indexOf("/");
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		tx.downloadDirectory(bucketName, key, new File(System.getProperty("user.dir")));
		return key;
	}
}
