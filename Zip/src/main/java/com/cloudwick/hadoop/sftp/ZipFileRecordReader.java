package com.cloudwick.hadoop.sftp;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ZipFileRecordReader extends RecordReader<Text, NullWritable> {

	private Text currentKey;
	private NullWritable currentValue = null;
	private boolean isFinished = false;

	@Override
	public void initialize(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {

		String[] sourceLocations = ((SFTPZipsListInputSplit) inputSplit)
				.getLocations();

		currentKey = new Text(sourceLocations[0]);

	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		System.out.println("isFinished status:" +isFinished );
		return isFinished ? 1 : 0;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		isFinished = true;
		System.out.println("set isFinished to true and sent key" + currentKey + " to mapper");
		return currentKey;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		System.out.println("returned currentValue as null");
		return currentValue;
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
