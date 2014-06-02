package de.huberlin.wbi.hiway.common;

public class FileStat {

	private String fileName;
	private long realTime;
	private long size;

	public FileStat() {
	}

	public FileStat(long size, long realTime, String fileName) {
		this.size = size;
		this.realTime = realTime;
		this.fileName = fileName;
	}

	public FileStat(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return this.fileName;
	}

	public long getRealTime() {
		return this.realTime;
	}

	public long getSize() {
		return this.size;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setRealTime(long realtime) {
		this.realTime = realtime;
	}

	public void setSize(long size) {
		this.size = size;
	}

}
