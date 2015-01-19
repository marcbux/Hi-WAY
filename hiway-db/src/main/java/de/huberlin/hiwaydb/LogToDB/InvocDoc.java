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
package de.huberlin.hiwaydb.LogToDB;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;




public class InvocDoc {

	private Long timestamp;
	private String runId;
	private Long invocId;
	private String taskname;
	private Long taskId;
	private String lang;

	private String hostname;
	private Long scheduleTime;
	private String standardError;
	private String standardOut;
	
	private Long realTimeIn;
	private Long realTimeOut;
	private Long realTime;
	
	private Set<String> userevents = new HashSet<String>(0);
	private Map<String, String>  input = new HashMap<String, String> (0);
	private Map<String, String> output = new HashMap<String, String> (0);
	private Map<String, HashMap<String,Long>> files = new HashMap<String, HashMap<String,Long>>();
	

	public 	Map<String, HashMap<String,Long>> getFiles() {
		return this.files;
	}

	public void setFiles(Map<String, HashMap<String,Long>> files) {
		this.files = files;
	}
	
	
	public Map<String, String> getOutput() {
		return this.output;
	}

	public void setOutput(Map<String, String> output) {
		this.output = output;
	}

	public  Map<String, String> getInput() {
		return this.input;
	}

	public void setInput (Map<String, String> input) {
		this.input = input;
	}
	
	public Set<String> getUserevents() {
		return this.userevents;
	}

	public void setUserevents(Set<String> userevents) {
		this.userevents = userevents;
	}


	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public Long getTaskId() {
		return taskId;
	}

	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}

	public String getTaskname() {
		return taskname;
	}

	public void setTaskname(String taskname) {
		this.taskname = taskname;
	}

	public Long getInvocId() {
		return invocId;
	}

	public void setInvocId(Long invocId) {
		this.invocId = invocId;
	}

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		this.runId = runId;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public Long getScheduleTime() {
		return scheduleTime;
	}

	public void setScheduleTime(Long scheduleTime) {
		this.scheduleTime = scheduleTime;
	}

	public String getStandardError() {
		return standardError;
	}

	public void setStandardError(String standardError) {
		this.standardError = standardError;
	}

	public String getStandardOut() {
		return standardOut;
	}

	public void setStandardOut(String standardOut) {
		this.standardOut = standardOut;
	}

	public Long getRealTimeIn() {
		return realTimeIn;
	}

	public void setRealTimeIn(Long realTimeIn) {
		this.realTimeIn = realTimeIn;
	}

	public Long getRealTimeOut() {
		return realTimeOut;
	}

	public void setRealTimeOut(Long realTimeOut) {
		this.realTimeOut = realTimeOut;
	}

	public Long getRealTime() {
		return realTime;
	}

	public void setRealTime(Long realTime) {
		this.realTime = realTime;
	}

}
