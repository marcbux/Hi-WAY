/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
 * Hannes Schuh (HU Berlin)
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
package de.huberlin.wbi.hiway.logstats;

public class Invocation {

	private long execTime;
	private long execTimestamp;

	private String hostName;
	private long schedTime;
	private long shutdownTimestamp;
	private long stageinTime;
	private long stageinTimestamp;
	private long stageoutTime;
	private long stageoutTimestamp;
	private long startupTimestamp;
	private String taskName;

	private double totalFileSize = 1d;

	public Invocation(String taskName) {
		this.taskName = taskName;
	}

	public void addFileSize(long fileSize) {
		this.totalFileSize *= fileSize;
	}

	public long getExecTime() {
		return execTime;
	}

	public long getExecTimestamp() {
		return execTimestamp;
	}

	public double getFileSize() {
		return totalFileSize;
	}

	public String getHostName() {
		return hostName;
	}

	public long getSchedTime() {
		return schedTime;
	}

	public long getShutdownTime() {
		return shutdownTimestamp - stageoutTimestamp - stageoutTime;
	}

	public long getStageinTime() {
		return stageinTime;
	}

	public long getStageoutTime() {
		return stageoutTime;
	}

	public long getStartupTime() {
		return stageinTimestamp - startupTimestamp - schedTime;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setExecTime(long execTime) {
		this.execTime = execTime;
	}

	public void setExecTimestamp(long execTimestamp) {
		this.execTimestamp = execTimestamp;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public void setSchedTime(long schedTime) {
		this.schedTime = schedTime;
	}

	public void setShutdownTimestamp(long shutdownTimestamp) {
		this.shutdownTimestamp = shutdownTimestamp;
	}

	public void setStageinTime(long stageinTime) {
		this.stageinTime = stageinTime;
	}

	public void setStageinTimestamp(long stageinTimestamp) {
		this.stageinTimestamp = stageinTimestamp;
	}

	public void setStageoutTime(long stageoutTime) {
		this.stageoutTime = stageoutTime;
	}

	public void setStageoutTimestamp(long stageoutTimestamp) {
		this.stageoutTimestamp = stageoutTimestamp;
	}

	public void setStartupTimestamp(long startupTimestamp) {
		this.startupTimestamp = startupTimestamp;
	}

}
