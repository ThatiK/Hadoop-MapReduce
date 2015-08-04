package com.cloudwick.hadoop.Zip;

import java.io.IOException; 

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit; 
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 

public class ZipFileInputFormat extends FileInputFormat<Text,BooleanWritable>{
	
	@Override
	/**
	 * We tell hadoop not to split the zip file.
	 */
	protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext ctx, Path filename){
			return false;
	}
 
 
	@Override
	public RecordReader<Text,BooleanWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException{
			return new ZipFileRecordReader();
	}		
}