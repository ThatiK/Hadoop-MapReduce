package com.cloudwick.hadoop.Zip;

import java.io.IOException; 
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils; 
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.CompressionCodecFactory; 

public class HDFSClient {
	private Configuration hdfsConfig;

	public Configuration getHdfsConfig() {
		return hdfsConfig;
	}

	public void setHdfsConfig(Configuration hdfsConfig) {
		this.hdfsConfig = hdfsConfig;
	}

	public HDFSClient(Configuration hdfsConfig) {
		super();
		this.hdfsConfig = hdfsConfig;
	}

	public HDFSClient() {
		this.hdfsConfig = new Configuration();
	}

	public boolean copyFromStream(ZipInputStream zipInputStream, String destination)
			throws IOException {
		CompressionCodec compressionCodec = null;
		CompressionCodecFactory compressionCodecFactory = null;

		FSDataOutputStream fsDataOutputStream = null;
		CompressionOutputStream compressionOutputStream = null;
		
		boolean success = false;

		// Compressor gzipCompressor = null;
		// OutputStream compressedOut = null;

		try {
			FileSystem fileSystem = FileSystem.get(this.getHdfsConfig());
			compressionCodecFactory = new CompressionCodecFactory(
					this.getHdfsConfig());
			Path destPath = new Path(destination+ ".gz");

			System.out.println("destination path: " + destPath.toString());

			fsDataOutputStream = fileSystem.create(destPath , true, 131072);

			compressionCodec = compressionCodecFactory.getCodec(destPath);
			compressionOutputStream = compressionCodec
					.createOutputStream(fsDataOutputStream);

			// GzipCodec gzipCodec = (GzipCodec) ReflectionUtils.newInstance(
			// GzipCodec.class, this.getHdfsConfig());
			// gzipCompressor = CodecPool.getCompressor(gzipCodec);
			// compressedOut = gzipCodec.createOutputStream(
			// fsDataOutputStream, gzipCompressor);

			IOUtils.copyBytes(zipInputStream, compressionOutputStream, 131072, false);
			success = true;
			
			
			
			// compressionOutputStream.finish();
		} catch (IOException e) {
			throw e;
		} finally {
			 if (compressionOutputStream != null) {
			 compressionOutputStream.close();
			 System.out.println("compressionOutputStream closed in finally");
			 }
			 if (fsDataOutputStream != null) {
			 fsDataOutputStream.close();
			 System.out.println("fsDataOutputStream closed in finally");
			 }
			// CodecPool.returnCompressor(gzipCompressor);
			// compressedOut.close();
			// fsDataOutputStream.close();
			IOUtils.closeStream(fsDataOutputStream);
			System.out.println("ioutils stream closed in finally");
		}
		IOUtils.closeStream(fsDataOutputStream);
		System.out.println("ioutils stream closed");
		return success;
	}
}
