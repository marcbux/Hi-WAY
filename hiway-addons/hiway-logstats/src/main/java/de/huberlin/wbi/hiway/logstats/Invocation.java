/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Jörgen Brandt (HU Berlin)
 * Marc Bux (HU Berlin)
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

import java.util.Comparator;

public class Invocation {

	// information obtained from container-allocated
	private Container container;
	private String hostName;

	private long execFinishTimestamp;

	private long execOnsetTimestamp;
	
	private long schedTime;
	
	private double fileSize = 1d;
	
	// information obtained from invoc-exec
	private String taskName;

	public Invocation(String taskName) {
		this.taskName = taskName;
	}

	public long getExecTime() {
		return execFinishTimestamp - execOnsetTimestamp;
	}

	public long getSchedTime() {
		return schedTime;
	}

	public long getShutdownTime() {
		return container.getCompletedTimestamp() - execFinishTimestamp;
	}

	public long getStartupTime() {
		return execOnsetTimestamp - container.getAllocatedTimestamp()
				- schedTime;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setContainer(Container container) {
		this.container = container;
	}

	public void setExecFinishTimestamp(long execFinishTimestamp) {
		this.execFinishTimestamp = execFinishTimestamp;
	}

	public void setExecOnsetTimestamp(long execOnsetTimestamp) {
		this.execOnsetTimestamp = execOnsetTimestamp;
	}

	public void setSchedTime(long schedTime) {
		this.schedTime = schedTime;
	}
	
	public String getHostName() {
		return hostName;
	}
	
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	
	public long getExecOnsetTimestamp() {
		return execOnsetTimestamp;
	}
	
	public double getFileSize() {
		return fileSize;
	}
	
	public void addFileSize(long fileSize) {
		this.fileSize *= fileSize;
	}
	
}