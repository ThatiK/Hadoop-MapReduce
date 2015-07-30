package com.cloudwick.hadoop.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class ZipFileRecordReader extends RecordReader<Text, BooleanWritable> {
	private static final String path_sftp_properties = "config.properties";

	private ZipInputStream zip;
	private Text currentKey;
	private BooleanWritable currentValue;
	private boolean isFinished = false;
	private Configuration conf;
	private Properties props;
	private Session session;
	private ChannelSftp sftpChannel;
	
	
	/*
	 * For ach split (1 mapper for each zip file) makes a sftp connection.
	 * Fetches the stream for each zip file. Zip stream is closed in close()
	 * method.
	 */
	@Override
	public void initialize(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		conf = taskAttemptContext.getConfiguration();

		String[] sourceName = ((SFTPZipsListInputSplit) inputSplit)
				.getLocations();
		props = new Properties();
		props.load(getClass().getClassLoader().getResourceAsStream(
				path_sftp_properties));
		try {
			zip = new ZipInputStream(getZipInputStream(sourceName[0]));
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public InputStream getZipInputStream(String sourceName)
			throws JSchException, SftpException {
		JSch jsch = new JSch();

		session = jsch.getSession(props.getProperty("source-user"),
				props.getProperty("source-host"),
				Integer.parseInt(props.getProperty("source-port", "22")));

		session.setPassword(props.getProperty("source-password"));
		Properties sftpConf = new Properties();
		sftpConf.put("StrictHostKeyChecking", "no");
		session.setConfig(sftpConf);
		session.connect();
		sftpChannel = (ChannelSftp) session.openChannel("sftp");
		sftpChannel.connect();
		return sftpChannel.get(props.getProperty("source-directory")
				+ sourceName);
	}

	/*
	 * No filter by filename inside zip files--start
	 * 
	 * @Override public boolean nextKeyValue() throws IOException,
	 * InterruptedException { ZipEntry entry = zip.getNextEntry(); if ( entry ==
	 * null ) { isFinished = true; return false; } // Set the key currentKey =
	 * new Text( entry.getName() );
	 * 
	 * // Set the value ByteArrayOutputStream bos = new ByteArrayOutputStream();
	 * byte[] temp = new byte[8192]; while ( true ) { int bytesRead =
	 * zip.read(temp, 0, 8192); if ( bytesRead > 0 ) bos.write(temp, 0,
	 * bytesRead); else break; } zip.closeEntry(); currentValue = new
	 * BytesWritable( bos.toByteArray() ); return true; } No filter by filename
	 * inside zip files--end
	 */

	/* Filter files by filename inside zip files--start */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return getNextKey();

	}

	public boolean getNextKey() throws IOException {
		ZipEntry entry = zip.getNextEntry();
		if (entry == null) {
			isFinished = true;
			return false;
		} else
			return patternEntryCheck(entry);
	}

	public boolean patternEntryCheck(ZipEntry entry) throws IOException {
		String fileName = entry.getName();

		// file.pattern passed as -Dfile.pattern=pattern
		String filePattern = conf.get("file.pattern");
		Pattern pattern = null;
		Matcher matcher = null;

		if (filePattern != null) {
			pattern = Pattern.compile(filePattern.toLowerCase().trim());
			matcher = pattern.matcher(fileName.toLowerCase());
		}

		if (matcher != null && matcher.find()) {
			System.out.println("pattern entry check passed -- file name: "
					+ fileName);
			// Set the key
			currentKey = new Text(fileName);

			try {
				currentValue = new BooleanWritable((new SFTPCopy()).doCopy(
						currentKey.toString(), zip, conf));
			} catch (JSchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SftpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			zip.closeEntry();
			return true;
		} else {
			// if file not matched with pattern skip the file and move to next
			// file
			return getNextKey();
		}
	}

	/* Filter files by filename inside zip --end */

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return isFinished ? 1 : 0;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public BooleanWritable getCurrentValue() throws IOException,
			InterruptedException {
		return currentValue;
	}

	@Override
	public void close() throws IOException {
		try {
			zip.close();
			System.out.println("zip input stream closed in record reader");
			if ((session != null) && (session.isConnected()))
				session.disconnect();
		} catch (Exception e) {
		}
	}

}
