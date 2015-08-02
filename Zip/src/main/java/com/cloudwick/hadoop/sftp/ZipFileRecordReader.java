package com.cloudwick.hadoop.sftp;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ZipFileRecordReader extends RecordReader<Text, BooleanWritable> {
	private static final String path_sftp_properties = "config.properties";
	private Text currentKey;
	private NullWritable currentValue = null;
	private boolean isFinished = false;
	private Properties props;
	private Configuration conf;
	private TaskAttemptContext taskAttemptContext;

	@Override
	public void initialize(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		conf = taskAttemptContext.getConfiguration();
		this.taskAttemptContext = taskAttemptContext;

		String[] sourceLocations = ((SFTPZipsListInputSplit) inputSplit)
				.getLocations();
		
		String zipFileName =sourceLocations[0];

		props = new Properties();
		props.load(getClass().getClassLoader().getResourceAsStream(
				path_sftp_properties));

		InetAddress thisIp = InetAddress.getLocalHost();
		System.out.println("Copying " + zipFileName + " on " + thisIp.getHostAddress());
		
		currentKey = new Text(sourceLocations[0]);

	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		System.out.println("isFinished status:" +isFinished );
		return isFinished ? 1 : 0;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException { 
		return currentKey;
	}

	@Override
	public BooleanWritable getCurrentValue() throws IOException,
			InterruptedException { 
		try {
			isFinished = true;
			System.out.println("sending "+ currentKey + " for copy command");
			boolean result = (Boolean) new RetriableSFTPCopyCommand("SFTP Copy " + currentKey)
					.execute(currentKey.toString(), props, conf);
			return new BooleanWritable(result);
		} catch (Exception ex) {
			taskAttemptContext.setStatus("Copy Failure: " + currentKey);
			throw new IOException("File copy failed: " + currentKey + "-->"
					+ props.getProperty("source-directory"), ex);
		}
	}
 

	@Override
	public void close() throws IOException {

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(isFinished){
			System.out.println("returning false since no more keys");
			return false;
		}
		else{
			System.out.println("returning true to send currentkey");
			return true;
		}
	}

}
