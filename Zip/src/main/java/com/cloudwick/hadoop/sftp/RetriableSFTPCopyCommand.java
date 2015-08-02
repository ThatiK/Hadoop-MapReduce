package com.cloudwick.hadoop.sftp;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import org.apache.hadoop.tools.util.RetriableCommand;

public class RetriableSFTPCopyCommand extends RetriableCommand {
	private Configuration hdfsConf;
	private ZipInputStream zip;
	private boolean currentCopySucess = false; 
	Session session = null;
	ZipEntry entry = null;

	public RetriableSFTPCopyCommand(String description) {
		super(description);
	}

	@Override
	protected Object doExecute(Object... arguments) throws Exception {
		// TODO Auto-generated method stub

		assert arguments.length == 3 : "Unexpected argument list.";
		String zipFileName = (String) arguments[0];
		Properties props = (Properties) arguments[1];
		hdfsConf = (Configuration) arguments[2];

		try {
			JSch jsch = new JSch();

			session = jsch.getSession(props.getProperty("source-user"),
					props.getProperty("source-host"),
					Integer.parseInt(props.getProperty("source-port", "22")));

			session.setPassword(props.getProperty("source-password"));
			Properties sftpConf = new Properties();
			sftpConf.put("StrictHostKeyChecking", "no");
			session.setConfig(sftpConf);
			session.connect();
			System.out.println("session opened for this map");
			ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
			sftpChannel.connect();
			System.out.println("sftp channel connected for this map");
			System.out.println("source directory " + props.getProperty("source-directory"));
			System.out.println("zip file name " + zipFileName);
			zip = new ZipInputStream(sftpChannel.get(props
					.getProperty("source-directory") + zipFileName));

			try {
				while ((entry = zip.getNextEntry()) != null) {
					patternEntryCheck(entry);
				}
			} catch (Exception ex) {
				throw ex;
			} finally {
				this.close();
			} 

			if (currentCopySucess)
				return true;
			else
				return false;

		} catch (Exception ex) {
			if ((session != null) && (session.isConnected())) {
				session.disconnect();
				System.out.println("session disconnected in catch exception");
			}

			throw (ex);
		}
	} 

	public void patternEntryCheck(ZipEntry entry) throws IOException,
			JSchException, SftpException {
		String fileName = entry.getName();
		System.out.println("file to check for pattern: " + fileName);

		// file.pattern passed as -Dfile.pattern=pattern
		String filePattern = hdfsConf.get("file.pattern");
		Pattern pattern = null;
		Matcher matcher = null;

		if (filePattern != null) {
			pattern = Pattern.compile(filePattern.toLowerCase().trim());
			matcher = pattern.matcher(fileName.toLowerCase());
		}

		if (matcher != null && matcher.find()) {
			System.out.println("pattern entry check passed -- file name: "
					+ fileName);
			currentCopySucess = false;
			currentCopySucess = doCopy(fileName, zip, hdfsConf);
			zip.closeEntry(); 
		}
		else
			System.out.println("pattern entry check failed -- file name: "
					+ fileName);

	}

	public boolean doCopy(String fileName, ZipInputStream zipInputStream,
			Configuration conf) throws JSchException, SftpException,
			IOException {

		HDFSClient hdfsClient = new HDFSClient(conf);
		Path path = new Path(conf.get("destPath"));

		hdfsClient.copyFromStream(zipInputStream, path + "/" + fileName);

		return true;

	}

	public void close() throws IOException {
		if (zip != null) {
			zip.close();
			System.out
					.println("zip input stream closed in RetriableSFTPCopyCommand");
		}

		if ((session != null) && (session.isConnected())) {
			session.disconnect();
			System.out.println("session in RetriableSFTPCopyCommand closed");
		}

	}

	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
		System.out.println("finalize in RetriableSFTPCopyCommand is called");
	}

	public static class CopyReadException extends IOException {
		public CopyReadException(String e) {
			super(e);
		}

		public CopyReadException(Throwable rootCause) {
			super(rootCause);
		}
	}

}
