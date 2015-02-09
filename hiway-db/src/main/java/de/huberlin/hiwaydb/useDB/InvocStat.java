/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Hannes Schuh (HU Berlin)
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
 * Ulf Leser (HU Berlin)
 *
 * Jörgen Brandt is funded by the European Commission through the BiobankCloud
 * project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 * research training group SOAMED (GRK 1651).
 *
 * Copyright 2014 Humboldt-Universität zu Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.huberlin.hiwaydb.useDB;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class InvocStat {

	private String hostName;
	private Map<String, FileStat> inputFiles;
	private Map<String, FileStat> outputFiles;
	private Long realTime;

	private final long taskId;
	private Long timestamp;
	private final String runId;

	public InvocStat(String runId, long taskId) {
		this.timestamp = -1l;
		this.taskId = taskId;
		this.runId = runId;

		inputFiles = new HashMap<>();
		outputFiles = new HashMap<>();
	}

	public InvocStat(long timestamp, Long realTime, String runId, long taskId, String hostName) {
		this(runId, taskId);

		this.timestamp = timestamp;
		this.realTime = realTime;
		this.hostName = hostName;
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

	public String getHostName() {
		return this.hostName;
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

	public Long getRealTime() {
		return this.realTime;
	}

	public long getTaskId() {
		return this.taskId;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
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

	public void setRealTime(Long d, Long timestamp) {
		this.realTime = d;
		setTimestamp(timestamp);
	}

	private void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("{Timestamp: " + this.timestamp + " | Host: " + this.hostName + " | TaskID: " + this.taskId + " | Time: " + this.realTime + " | RunID: "
				+ this.runId + " FilesIn: " + this.getInputFiles().size() + " FilesOut:" + this.getOutputFiles().size() + " }");

		return sb.toString();
	}

	public String getRunId() {
		return runId;
	}

}
