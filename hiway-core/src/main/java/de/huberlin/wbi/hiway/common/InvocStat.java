package de.huberlin.wbi.hiway.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class InvocStat {

	private String hostname;
	private Map<String, FileStat> inputFiles;
	private Map<String, FileStat> outputFiles;
	private double realTime;

	private long taskId;
	private long timestamp;

	public InvocStat() {
		inputFiles = new HashMap<>();
		outputFiles = new HashMap<>();
	}

	public InvocStat(long timestamp, long taskId) {
		this();
		this.timestamp = timestamp;
		this.taskId = taskId;
	}

	public InvocStat(long timestamp, long realTime, long taskId, String hostname) {
		this(timestamp, taskId);
		this.realTime = realTime;
		this.hostname = hostname;
	}

	public void addInputFile(FileStat inputFile) {
		inputFiles.put(inputFile.getFileName(), inputFile);
	}

	public void addOutputFile(FileStat outputFile) {
		outputFiles.put(outputFile.getFileName(), outputFile);
	}

	public boolean containsInputFile(String fileName) {
		return inputFiles.containsKey(fileName);
	}

	public boolean containsOutputFile(String fileName) {
		return outputFiles.containsKey(fileName);
	}

	public String getHostname() {
		return this.hostname;
	}

	public FileStat getInputFile(String fileName) {
		return inputFiles.get(fileName);
	}

	public Collection<FileStat> getInputFiles() {
		return inputFiles.values();
	}

	public FileStat getOutputFile(String fileName) {
		return outputFiles.get(fileName);
	}

	public Collection<FileStat> getOutputFiles() {
		return outputFiles.values();
	}

	public double getRealTime() {
		return this.realTime;
	}

	public long getTaskId() {
		return this.taskId;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setInputfiles(Collection<FileStat> inputFiles) {
		for (FileStat inputFile : inputFiles) {
			addInputFile(inputFile);
		}
	}

	public void setOutputfiles(Collection<FileStat> outputFiles) {
		for (FileStat outputFile : outputFiles) {
			addOutputFile(outputFile);
		}
	}

	public void setRealTime(long realtime) {
		this.realTime = realtime;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
