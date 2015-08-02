package com.cloudwick.hadoop.sftp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class CopyOfSFTPZipsListInputSplit extends InputSplit implements Writable {
	private List<String> sourceNames;

	public CopyOfSFTPZipsListInputSplit() {
		// TODO Auto-generated constructor stub
	}

	public CopyOfSFTPZipsListInputSplit(List<String> sources) {
		this.sourceNames = sources;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return sourceNames.size();
	}

	/* Source is at single location so the 1st index contains the source name */
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new String[] {};
	}

	public List<String> getSources() {
		return sourceNames;
	}

	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		List<String> inputList = new ArrayList<String>();
		int count = input.readInt();
		for(int i=0;i<count;i++){
			inputList.add(input.readUTF());
		}
		this.sourceNames = inputList;

	}

	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeInt(this.sourceNames.size());
		for(String source: this.sourceNames){
			output.writeUTF(source);
		}
	}

}
