package com.cloudwick.hadoop.sftp;

import java.io.IOException; 

import org.apache.hadoop.io.BooleanWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 

public class ZipMapper extends Mapper<Text, BooleanWritable, Text, Text> {  
	private final static Text trueStr = new Text("Success");
	private final static Text failStr = new Text("Failed");
	
	@Override
	protected void map(Text key, BooleanWritable value,
			Mapper<Text, BooleanWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// super.map(key, value, context); 
		
		 
		
		BooleanWritable success  = value;
		
		if(success.get()) 
			context.write((Text)key, trueStr);
		else
			context.write((Text)key, failStr);
		
//		String fileContent = new String(value.getBytes(), "UTF-8");
//		String[] lines = fileContent.split("\n");
//
//		for (int i = 0; i < lines.length; i++) { 
//			context.write(new Text(key), new Text(lines[i]));
//			//this counter will has group name specified as argument 1 and increments records corresponding with each filename.
//			//Existence of keys and incrementing is taken care here
//			//This group counter will will be printed out by deafult
//			context.getCounter("Number of records for each file", key.toString()).increment(1);
//		} 
	}

}
