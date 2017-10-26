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
package de.huberlin.wbi.hiway.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONException;

import de.huberlin.hiwaydb.useDB.FileStat;
import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.hiwaydb.useDB.InvocStat;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public class ProvenanceManager implements HiwayDBI {

	private Set<String> hostNames;
	private Map<UUID, Map<Long, InvocStat>> runToInvocStats;
	private Map<UUID, String> runToWorkflowName;
	private Map<Long, String> taskIdToTaskName;

	private Map<String, Set<Long>> workflowNameToTaskIds;

	public ProvenanceManager() {
		runToWorkflowName = new HashMap<>();
		workflowNameToTaskIds = new HashMap<>();
		taskIdToTaskName = new HashMap<>();
		hostNames = new HashSet<>();
		runToInvocStats = new HashMap<>();
	}

	@Override
	public Set<String> getHostNames() {
		return new HashSet<>(hostNames);
	}

	private Collection<InvocStat> getLogEntriesForTask(long taskId) {
		Collection<InvocStat> stats = new LinkedList<>();
		for (String hostName : getHostNames()) {
			stats.addAll(getLogEntriesForTaskOnHostSince(taskId, hostName, 0l));
		}
		return stats;
	}

	@Override
	public synchronized Collection<InvocStat> getLogEntriesForTaskOnHostSince(long taskId, String hostName, long timestamp) {
		Collection<InvocStat> stats = new LinkedList<>();
		for (UUID runKey : runToInvocStats.keySet()) {
			Map<Long, InvocStat> invocStats = runToInvocStats.get(runKey);
			for (Long statKey : invocStats.keySet()) {
				InvocStat stat = invocStats.get(statKey);
				if (stat.getHostName() != null && stat.getTaskId() == taskId && stat.getHostName().equals(hostName) && stat.getTimestamp() > timestamp) {
					stats.add(stat);
				}
			}
		}
		return stats;
	}

	@Override
	public Collection<InvocStat> getLogEntriesForTasks(Set<Long> taskIds) {
		Collection<InvocStat> stats = new LinkedList<>();
		for (long taskId : taskIds) {
			stats.addAll(getLogEntriesForTask(taskId));
		}
		return stats;
	}

	@Override
	public Set<Long> getTaskIdsForWorkflow(String workflowName) {
		return workflowNameToTaskIds.containsKey(workflowName) ? new HashSet<>(workflowNameToTaskIds.get(workflowName)) : new HashSet<>();
	}

	@Override
	public String getTaskName(long taskId) {
		return taskIdToTaskName.get(taskId);
	}

	@Override
	public synchronized void logToDB(JsonReportEntry entry) {
		Long invocId = entry.getInvocId();
		UUID runId = entry.getRunId();
		String fileName = entry.getFile();

		if (!runToInvocStats.containsKey(runId)) {
			Map<Long, InvocStat> invocStats = new HashMap<>();
			runToInvocStats.put(runId, invocStats);
		}

		if (invocId != null && !runToInvocStats.get(runId).containsKey(invocId)) {
			InvocStat invocStat = new InvocStat(entry.getRunId().toString(), entry.getTaskId());
			workflowNameToTaskIds.get(runToWorkflowName.get(runId)).add(entry.getTaskId());
			taskIdToTaskName.put(entry.getTaskId(), entry.getTaskName());
			runToInvocStats.get(runId).put(invocId, invocStat);
		}
		InvocStat invocStat = runToInvocStats.get(runId).get(invocId);

		switch (entry.getKey()) {
		case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
		case HiwayDBI.KEY_FILE_TIME_STAGEIN:
			if (!invocStat.containsInputFile(fileName)) {
				FileStat fileStat = new FileStat(fileName);
				invocStat.addInputFile(fileStat);
			}
			break;
		case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
		case HiwayDBI.KEY_FILE_TIME_STAGEOUT:
			if (!invocStat.containsOutputFile(fileName)) {
				FileStat fileStat = new FileStat(fileName);
				invocStat.addOutputFile(fileStat);
			}
			break;
		default:
		}

		try {
			switch (entry.getKey()) {
			case HiwayDBI.KEY_WF_NAME:
				runToWorkflowName.put(runId, entry.getValueRawString());
				String workflowName = entry.getValueRawString();
				if (!workflowNameToTaskIds.containsKey(workflowName)) {
					Set<Long> taskIds = new HashSet<>();
					workflowNameToTaskIds.put(workflowName, taskIds);
				}
				break;
			case JsonReportEntry.KEY_INVOC_TIME:
				invocStat.setRealTime(entry.getValueJsonObj().getLong("realTime"), entry.getTimestamp());
				break;
			case HiwayDBI.KEY_INVOC_HOST:
				String hostName = entry.getValueRawString();
				invocStat.setHostName(hostName);
				hostNames.add(hostName);
				break;
			case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
				invocStat.getInputFile(fileName).setSize(Long.parseLong(entry.getValueRawString()));
				break;
			case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
				invocStat.getOutputFile(fileName).setSize(Long.parseLong(entry.getValueRawString()));
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEIN:
				invocStat.getInputFile(fileName).setRealTime((entry.getValueJsonObj().getLong("realTime")));
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEOUT:
				invocStat.getOutputFile(fileName).setRealTime((entry.getValueJsonObj().getLong("realTime")));
				break;
			default:
			}
		} catch (JSONException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
	}

}
