import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import com.amazonaws.services.s3.sample.PutS3ObjectSample;
import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.DynamicConfiguration;
import com.sap.aii.mapping.api.DynamicConfigurationKey;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;

public class CustomRestHeader extends AbstractTransformation {
	DynamicConfiguration dy;
	String filename;
	private String auth;
	private String date;
	private String sha256;
	private String contenLenght;
	private String storageClass;
	private String endurl;

	private static final String awsAccessKey = "";

	/** Put your secret key here **/
	private static final String awsSecretKey = "";

	/** Put your bucket name here **/
	private static final String bucketName = "";

	/** The name of the region where the bucket is created. (e.g. us-west-1) **/
	private static final String regionName = "";

	public void transform(TransformationInput in, TransformationOutput out)
			throws StreamTransformationException {
		try {
			String inputPayload = "";
			String line = "";
			String finalFileName = "";
			dy = in.getDynamicConfiguration();
			DynamicConfigurationKey dyKeyFileName = DynamicConfigurationKey
					.create("http://sap.com/xi/XI/System/File", "FileName");
			DynamicConfigurationKey dyKeyDate = DynamicConfigurationKey.create(
					"http://sap.com/xi/XI/System/REST", "date");
			DynamicConfigurationKey dyKeyAuth = DynamicConfigurationKey.create(
					"http://sap.com/xi/XI/System/REST", "auth");
			DynamicConfigurationKey dyKeyContentSha256 = DynamicConfigurationKey
					.create("http://sap.com/xi/XI/System/REST", "sha256");
			DynamicConfigurationKey dyKeyContenLength = DynamicConfigurationKey
					.create("http://sap.com/xi/XI/System/REST", "contenLenght");
			DynamicConfigurationKey dyKeyStorageClass = DynamicConfigurationKey
					.create("http://sap.com/xi/XI/System/REST", "storageclass");
			DynamicConfigurationKey dyKeyfileName1 = DynamicConfigurationKey
					.create("http://sap.com/xi/XI/System/REST", "name");
			DynamicConfigurationKey dyKeyUrl = DynamicConfigurationKey
			.create("http://sap.com/xi/XI/System/REST", "url");
			
			String fileName = dy.get(dyKeyFileName);
			//String fileName = "dummy.txt";
			
			finalFileName = "FolderName" + fileName;
			getTrace().addInfo("FileName " + finalFileName);

			InputStream is = in.getInputPayload().getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null)
				inputPayload += line;
			getTrace().addInfo("payload"+inputPayload);
			Map<String, String> header = PutS3ObjectSample.putS3Object(
					bucketName, regionName, awsAccessKey, awsSecretKey,
					inputPayload, finalFileName);
			auth = header.get("Authorization");
			contenLenght = header.get("content-length");
			storageClass = header.get("x-amz-storage-class");
			date = header.get("x-amz-date");
			sha256 = header.get("x-amz-content-sha256");
			//endurl = header.get("endurl");
			dy.put(dyKeyAuth, auth);
			dy.put(dyKeyContenLength, contenLenght);
			dy.put(dyKeyStorageClass, storageClass);
			dy.put(dyKeyDate, date);
			dy.put(dyKeyContentSha256, sha256);
			dy.put(dyKeyfileName1, fileName);
			//dy.put(dyKeyUrl, endurl);
			for ( String headerKey : header.keySet() ) {
				getTrace().addInfo(headerKey + ": " + header.get(headerKey));
			}
			
			getTrace().addInfo(endurl);
			out.getOutputPayload().getOutputStream().write(
					inputPayload.getBytes());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
