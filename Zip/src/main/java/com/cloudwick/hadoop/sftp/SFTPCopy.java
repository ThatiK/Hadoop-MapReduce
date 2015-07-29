package com.cloudwick.hadoop.sftp;

import java.io.IOException; 
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class SFTPCopy {
	public boolean doCopy(String fileName, ZipInputStream zipInputStream,
			Configuration conf) throws JSchException, SftpException,
			IOException {

		HDFSClient hdfsClient = new HDFSClient(conf);
		Path path = new Path(conf.get("destPath"));

		hdfsClient.copyFromStream(zipInputStream, path + "/" + fileName);

		return true;

	}
}
