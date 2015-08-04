package com.cloudwick.hadoop.binpack;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
 
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ZipMapper extends Mapper<Text, NullWritable, Text, Text> {
	private final static Text trueStr = new Text("Success");
	private final static Text failStr = new Text("Failed");
	private static final String path_sftp_properties = "config.properties";

	@Override
	protected void map(Text key, NullWritable value,
			Mapper<Text, NullWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// super.map(key, value, context);

		Properties props = new Properties();
		props.load(getClass().getClassLoader().getResourceAsStream(
				path_sftp_properties));

		InetAddress thisIp = InetAddress.getLocalHost();
		System.out.println("Copying " + key + " on " + thisIp.getHostAddress());

		boolean success = false;
		try {
			success = (Boolean) new RetriableSFTPCopyCommand("SFTP Copy " + key)
					.execute(key.toString(), props, context.getConfiguration());
		} catch (Exception ex) {
			context.setStatus("Copy Failure: " + key);
			throw new IOException("File copy failed: " + key + "-->"
					+ props.getProperty("source-dir"), ex);
		}

		// BooleanWritable success = value;

		if (success)
			context.write((Text) key, trueStr);
		else
			context.write((Text) key, failStr);

		// String fileContent = new String(value.getBytes(), "UTF-8");
		// String[] lines = fileContent.split("\n");
		//
		// for (int i = 0; i < lines.length; i++) {
		// context.write(new Text(key), new Text(lines[i]));
		// //this counter will has group name specified as argument 1 and
		// increments records corresponding with each filename.
		// //Existence of keys and incrementing is taken care here
		// //This group counter will will be printed out by deafult
		// context.getCounter("Number of records for each file",
		// key.toString()).increment(1);
		// }
	}

}
