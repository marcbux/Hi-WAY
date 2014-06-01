package de.huberlin.wbi.hiway.common;

public class FileStat {
	
	private long size;
	private long realTime;
	private String fileName;
	
	public FileStat() {}

	public FileStat(String fileName) {
		this.fileName = fileName;
	}
	
	public FileStat(long size, long realTime, String fileName) {
		this.size = size;
		this.realTime = realTime;
		this.fileName = fileName;
	}

	
	public long getSize() {
		return this.size;
	}

	public void setSize(long size) {
		this.size = size;
	}
	
	public String getFileName() {
		return this.fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getRealTime() {
		return this.realTime;
	}

	public void setRealTime(long realtime) {
		this.realTime = realtime;
	}

}
