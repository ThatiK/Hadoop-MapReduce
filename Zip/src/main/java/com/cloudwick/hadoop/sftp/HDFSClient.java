package com.cloudwick.hadoop.sftp;
 
import java.io.IOException; 
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSClient {
	private Configuration hdfsConfig;

	public Configuration getHdfsConfig() {
		return hdfsConfig;
	}

	public void setHdfsConfig(Configuration hdfsConfig) {
		this.hdfsConfig = hdfsConfig;
	}
	
	public HDFSClient(Configuration hdfsConfig){
		super();
		this.hdfsConfig = hdfsConfig;
	}

	public HDFSClient() {
		this.hdfsConfig = new Configuration();
	}

	public void copyFromStream(ZipInputStream zipInputStream, String destination)
			throws IOException {
		FileSystem fileSystem = FileSystem.get(this.getHdfsConfig());
		Path destPath = new Path(destination);
		
		System.out.println("destination path: "+destPath.toString());
		
		FSDataOutputStream fsdos = fileSystem.create(destPath, true, 131072);
		try {
			IOUtils.copyBytes(zipInputStream, fsdos, 131072,
					false);
		} catch (IOException e) {
			throw e;
		} finally {
			IOUtils.closeStream(fsdos);
		}
		IOUtils.closeStream(fsdos);
		System.out.println("ioutils stream closed");
	}
}
