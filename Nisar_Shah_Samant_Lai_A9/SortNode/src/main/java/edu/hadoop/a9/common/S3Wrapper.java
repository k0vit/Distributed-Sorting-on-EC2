package edu.hadoop.a9.common;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Wrapper {
	public S3Wrapper( AmazonS3 s3client ) {
		this.s3client = s3client;
	}
	
	public List<String> getListOfObjects( String bucketName , String prefix )
		throws AmazonServiceException {
		log.info(String.format("Requesting object listing for s3://%s/%s", bucketName, prefix));
		
		ListObjectsRequest request = new ListObjectsRequest();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);
		
		ArrayList<String> files = new ArrayList<String>();
		ObjectListing listing = null;
		do {
			listing = s3client.listObjects(request);
			for(S3ObjectSummary summary : listing.getObjectSummaries()){
				log.info("Object: " + summary.getKey());
				files.add(summary.getKey());
			}
		} while(listing.isTruncated());
		
		return files;
	}
	
	public InputStream getObjectInputStream( String bucketName , String objectId ) {
		log.info(String.format("Requesting for object: s3://%s/%s", bucketName, objectId));
		
		GetObjectRequest request = new GetObjectRequest(bucketName, objectId);
		S3Object object = s3client.getObject(request);
		ObjectMetadata md = object.getObjectMetadata();
		
		log.info(String.format("Content Type: %s", md.getContentType()));
		log.info(String.format("Content Length: %d", md.getContentLength()));
		
		return object.getObjectContent();
	}
	
	private static final Logger log = Logger.getLogger(S3Wrapper.class.getName());
	private AmazonS3 s3client;
}
