package com.cloudwick.hadoop.sftp;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;   
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
 
 
public class DriverManager extends Configured implements Tool { 
	 public int run(String[] args) throws Exception {

	        if (args.length != 2) {
	            System.out.printf(
	                    "Usage: %s [generic options] <gz output dir> <log output dir> \n", getClass()
	                    .getSimpleName());
	            ToolRunner.printGenericCommandUsage(System.out);
	            return -1;
	        }
	        
	        Configuration conf = new Configuration();
	        conf.set("mapreduce.job.queuename", "dev1");
	        conf.set("file.pattern", "gsod.*");
	        conf.set("mapreduce.map.tasks", "2");
	         
	        conf.set("destPath", args[0]);
	        Job job = Job.getInstance(conf);
	        job.setJarByClass(DriverManager.class);
	        job.setJobName(this.getClass().getName());
	        job.setNumReduceTasks(0);
 
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        job.setMapperClass(ZipMapper.class); 
	         
	        job.setInputFormatClass(ZipFileInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        
	        /*
	         * uncomment to get output format as gzip
	         */
//	        TextOutputFormat.setCompressOutput(job, true);
//	        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class); 

	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);

	        if (job.waitForCompletion(true)) { 
	            return 0;
	        }
	        return 1;
	    }

	    public static void main(String[] args) throws Exception {
	        int exitCode = ToolRunner.run(new DriverManager(), args);
	        System.exit(exitCode);
	    }

}