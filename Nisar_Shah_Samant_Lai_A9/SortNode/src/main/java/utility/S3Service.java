package utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/*
 * S3Service client which contains utility methods to list bucket data, 
 * stream data from a bucket.
 */
public class S3Service {
	private String bucketName;
	AmazonS3 s3client;

	/**
	 * 
	 * @param bucketName
	 * @param accessKey
	 * @param secretKey
	 */
	public S3Service(String bucketName, String accessKey, String secretKey) {
		super();
		this.bucketName = bucketName;
		this.s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
	}

	/**
	 *  This method lists the files in a particular bucket and the prefix is the prefix in the S3 file path.
	 * @param prefix
	 * @return
	 * @throws IOException
	 */
	public List<String> getData(String prefix) throws IOException {
		List<String> fileData = new ArrayList<String>();
		try {
			System.out.println("Listing objects");
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
	                .withBucketName(bucketName)
	                .withPrefix(prefix);
	            ObjectListing objectListing;            
	            do {
	                objectListing = s3client.listObjects(listObjectsRequest);
	                for (S3ObjectSummary objectSummary : 
	                	objectListing.getObjectSummaries()) {
//	                    System.out.println(" - " + objectSummary.getKey() + "  " +
//	                            "(size = " + objectSummary.getSize() + 
//	                            ")");
	                    fileData.add(objectSummary.getKey());
	                }
	                listObjectsRequest.setMarker(objectListing.getNextMarker());
	            } while (objectListing.isTruncated());
	            return fileData;        
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, " + "which means your request made it "
					+ "to Amazon S3, but was rejected with an error response " + "for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
		}
		return fileData;
		
	}

	/**
	 * Given an input stream which takes a GZ file it will display the file line by line.
	 * This can be modified to download the file to the local filesystem.
	 * @param input
	 * @throws IOException
	 */
	private static void displayTextInputStream(InputStream input) throws IOException {
		InputStream gzipStream = new GZIPInputStream(input);
		BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream));
		while (true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			System.out.println(" " + line);
		}
		System.out.println();
	}
	
	/**
	 * This will access data in the particular bucketName + filePath.
	 * @param filePath
	 * @throws IOException
	 */
	public void streamBucketData(String filePath) throws IOException {
		System.out.println("Downloading an object");
		S3Object s3object = s3client.getObject(new GetObjectRequest(bucketName, filePath));
		System.out.println("Content-Type: "  + 
        		s3object.getObjectMetadata().getContentType());
        displayTextInputStream(s3object.getObjectContent());
	}
}