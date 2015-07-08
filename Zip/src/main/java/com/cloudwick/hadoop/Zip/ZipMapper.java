package com.cloudwick.hadoop.Zip;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ZipMapper extends Mapper<Text, BytesWritable, Text, Text> {
	@Override
	protected void map(Text key, BytesWritable value,
			Mapper<Text, BytesWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// super.map(key, value, context);
		String content = new String(value.getBytes(), "UTF-8"); 
		
		// key is the filename inside zip file and value is the whole content inside the file inside zip
		context.write(key, new Text(content));
	}

}
