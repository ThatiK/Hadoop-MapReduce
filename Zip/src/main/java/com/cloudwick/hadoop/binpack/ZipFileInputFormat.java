package com.cloudwick.hadoop.binpack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

 
import org.apache.hadoop.fs.Path;   
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
  


import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class ZipFileInputFormat extends FileInputFormat<Text, NullWritable> {
	
	private static final String path_sftp_properties = "config.properties";
	
	@Override
	/**
	 * We tell hadoop not to split the zip file.
	 */
	protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext ctx,
			Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ZipFileRecordReader();
	}

	
	/*returns list of splits (each split referring to one zip file). 
	 * Since the config file has source-directory, all we need is the zip file name to fetch the data.
	 * We can create append location or any other information appended to the string and fetch the relevant information later.
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// TODO Auto-generated method stub
		// return super.getSplits(arg0);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		try {
			Properties prop = new Properties();
			prop.load(this.getClass().getClassLoader().getResourceAsStream(
					path_sftp_properties));
			
			System.out.println(prop.getProperty("source-user"));
			System.out.println(prop.getProperty("source-host"));
			System.out.println(prop.getProperty("source-password"));
			System.out.println(prop.getProperty("source-directory"));
			
			int numSplits = job.getConfiguration().getInt("mapreduce.map.tasks", 1);
			System.out.println("Number of mappers: "+numSplits);
			
			SFTPClient sftpClient = new SFTPClient(prop); 
			
			Map<String,Long> zipFiles = sftpClient.getZipFileNames(prop);
			Long sourceVolume = sftpClient.getSourceVolume();
			
			Long avgMapperVol =  (long) Math.ceil(sourceVolume / numSplits); 
			
			Map.Entry<String, Long> maxEntry = null;

			for (Map.Entry<String, Long> entry : zipFiles.entrySet())
			{
			    if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
			    {
			        maxEntry = entry;
			    }
			}
			
			Long maxZipFileVol = maxEntry.getValue();
			
			Long maxMapperVol;
			if(avgMapperVol < maxZipFileVol)
				maxMapperVol = maxZipFileVol; //worst scenario
			else
				maxMapperVol = avgMapperVol;
			
			
			
			FirstFitDecreasing ffd = new FirstFitDecreasing(zipFiles, maxMapperVol);
			long startTime;
	        long estimatedTime;

	        startTime = System.currentTimeMillis();
			List<Bin> bins = ffd.getResult(); 
			estimatedTime = System.currentTimeMillis() - startTime;
	        System.out.println("binpack algo time:  " + estimatedTime + " ms");
			for (Bin sources : bins) {
				 splits.add(new SFTPZipsListInputSplit(sources.toString()));
			}
			
//			int start =0;
//			int end =0;
//			for(int i=1;i<=numSplits;i++){
//				start=end;
//				if(i != numSplits){
//					end = (int) Math.ceil(i * fileNames.size() / numSplits);
//				}
//				else{
//					end = fileNames.size();
//				}
//				List<String> subList = fileNames.subList(start, end);
//				String sources = null;
//				
//				for(Object obj : subList){
//					sources = StringUtils.join(subList, ",");
//				}
//				splits.add(new SFTPZipsListInputSplit(sources));
//					
//			}
//			for (String fileName : fileNames) {
//				String sourceName = fileName;
//				splits.add(new SFTPZipsListInputSplit(sourceName));
//			}
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return splits;
	}
}