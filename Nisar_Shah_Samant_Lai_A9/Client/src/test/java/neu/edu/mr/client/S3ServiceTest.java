package neu.edu.mr.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;

import neu.edu.mr.utility.S3Service;

/**
 * Used to test the S3Service component
 * Usage: args: [access Key] [secret Key]
 * @author yuanjianlai
 *
 */
public class S3ServiceTest{
	
	public static void main(String[] args) throws IOException{
		BasicAWSCredentials awsCred = new BasicAWSCredentials(
				args[0],args[1]);
		S3Service s3 = new S3Service(awsCred);
		List<String> files = s3.getListOfObjects("s3://yuanjianlai.a6/output2/");
		for(String file:files){
			System.out.println(file);
		}
		InputStream input = s3.getObjectInputStream("s3://yuanjian.a8/output_1/part-00000");
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = null;
		while((line = reader.readLine())!=null){
			System.out.println(line);
		}
	}
	
}
