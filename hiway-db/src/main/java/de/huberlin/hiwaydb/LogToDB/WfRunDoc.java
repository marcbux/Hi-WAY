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

public class WfRunDoc {

	private String name;
	private String runId;
	private Long wfTime;
	private Long reductionTime;

	private Map<String, String> hiwayEvent = new HashMap<>(0);
	private Set<Long> taskIDs = new HashSet<>(0);

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		this.runId = runId;
	}

	public Long getWfTime() {
		return wfTime;
	}

	public void setWfTime(Long wfTime) {
		this.wfTime = wfTime;
	}

	public Map<String, String> getHiwayEvent() {
		return hiwayEvent;
	}

	public void setHiwayEvent(Map<String, String> hiwayEvent) {
		this.hiwayEvent = hiwayEvent;
	}

	public Long getReductionTime() {
		return reductionTime;
	}

	public void setReductionTime(Long reductionTime) {
		this.reductionTime = reductionTime;
	}

	public Set<Long> getTaskIDs() {
		return taskIDs;
	}

	public void setTaskIDs(Set<Long> taskIDs) {
		this.taskIDs = taskIDs;
	}

}
