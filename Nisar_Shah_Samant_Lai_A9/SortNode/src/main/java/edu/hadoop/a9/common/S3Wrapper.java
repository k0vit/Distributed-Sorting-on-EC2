package edu.hadoop.a9.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.opencsv.CSVWriter;

public class S3Wrapper {
	public S3Wrapper(AmazonS3 s3client) {
		this.s3client = s3client;
	}

	public List<String> getListOfObjects(String bucketName, String prefix) {
		log.info(String.format("Requesting object listing for s3://%s/%s", bucketName, prefix));

		ListObjectsRequest request = new ListObjectsRequest();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);

		ArrayList<String> files = new ArrayList<String>();
		ObjectListing listing = null;
		do {
			listing = s3client.listObjects(request);
			for (S3ObjectSummary summary : listing.getObjectSummaries()) {
				log.info("Object: " + summary.getKey());
				files.add(summary.getKey());
			}
		} while (listing.isTruncated());

		return files;
	}

	public InputStream getObjectInputStream(String bucketName, String objectId) {
		log.info(String.format("Requesting for object: s3://%s/%s", bucketName, objectId));

		GetObjectRequest request = new GetObjectRequest(bucketName, objectId);
		S3Object object = s3client.getObject(request);
		ObjectMetadata md = object.getObjectMetadata();

		log.info(String.format("Content Type: %s", md.getContentType()));
		log.info(String.format("Content Length: %d", md.getContentLength()));

		return object.getObjectContent();
	}

	public String readOutputFromS3(String outputPath, BasicAWSCredentials cred, String filename) {
		TransferManager tx = new TransferManager(cred);
		String simplifiedPath = (outputPath.replace("s3://", ""));
		int index = simplifiedPath.indexOf("/");
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		log.info(String.format("[%s] Downloaded file with Bucket Name: %s Key: %s ", filename, bucketName, key));
		log.info("CURRENT USER DIRECTORY: " + System.getProperty("user.dir"));
		Download d = tx.download(bucketName, key, new File(filename));
		try {
			d.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Failed downloading the file " + filename + ". Reason " + e.getMessage());
		}
		return filename;
	}

	/**
	 * 
	 * @param file
	 *            File to be uploaded.
	 * @param s3OutputPath
	 *            Path to be uploaded to.
	 * @return true if uploaded successfully.
	 */
	public boolean uploadFile(String file, String s3OutputPath) {
		File local = new File(file);
		if (!(local.exists() && local.canRead() && local.isFile())) {
			return false;
		}
		String folder = removeS3(s3OutputPath);
		String bucket = folder;
		String remote = local.getName();
		s3client.putObject(new PutObjectRequest(bucket, remote, local));
		return true;
	}

	private static String removeS3(String path) {
		if (!path.startsWith("s3://"))
			return path;
		return path.substring("s3://".length());
	}

	public boolean uploadDataToS3(String outputS3Path, ArrayList<String[]> nowSortedData, int instanceId) {
		String fileName = "part-r-" + instanceId + ".csv";
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(fileName));
			writer.writeAll(nowSortedData);
			writer.close();
			if (uploadFile(fileName, outputS3Path)) {
				return true;
			}
		} catch (IOException e) {
			log.severe(e.getMessage());
		}
		return false;
	}

	public String downloadAndStoreFileInLocal(String fileString, BasicAWSCredentials awsCredentials, String inputS3Path) {
		String s3FullPath = inputS3Path + "/" + fileString;
		log.info(String.format("[%s] Downloading from s3 full path: %s", fileString, s3FullPath));
		readOutputFromS3(s3FullPath, awsCredentials, fileString);
		return fileString;
	}

	private static final Logger log = Logger.getLogger(S3Wrapper.class.getName());
	private AmazonS3 s3client;

}
