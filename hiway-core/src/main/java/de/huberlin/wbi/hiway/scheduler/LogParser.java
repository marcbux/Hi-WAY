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
package de.huberlin.wbi.hiway.scheduler;

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
import de.huberlin.wbi.hiway.common.Constant;

public class LogParser implements HiwayDBI {

	private Set<String> hostNames;
	private Map<UUID, Map<Long, InvocStat>> runToInvocStats;
	private Map<UUID, String> runToWorkflowName;
	private Map<Long, String> taskIdToTaskName;
	private Map<String, Set<Long>> workflowNameToTaskIds;

	public LogParser() {
		runToWorkflowName = new HashMap<>();
		workflowNameToTaskIds = new HashMap<>();
		taskIdToTaskName = new HashMap<>();
		hostNames = new HashSet<>();
		runToInvocStats = new HashMap<>();
	}

	@Override
	public Set<String> getHostNames() {
		return hostNames;
	}

	@Override
	public Collection<InvocStat> getLogEntries() {
		return getLogEntriesSince(0l);
	}

	@Override
	public Collection<InvocStat> getLogEntriesForTask(long taskId) {
		return getLogEntriesSinceForTask(taskId, 0l);
	}

	@Override
	public Collection<InvocStat> getLogEntriesForTasks(Set<Long> taskId) {
		return getLogEntriesSinceForTasks(taskId, 0l);
	}

	@Override
	public Collection<InvocStat> getLogEntriesSince(long sinceTimestamp) {
		Set<Long> taskIds = new HashSet<>();
		for (Set<Long> newTaskIds : workflowNameToTaskIds.values()) {
			taskIds.addAll(newTaskIds);
		}
		return getLogEntriesSinceForTasks(taskIds, 0l);
	}

	@Override
	public Collection<InvocStat> getLogEntriesSinceForTask(long taskId,
			long sinceTimestamp) {
		Set<Long> taskIds = new HashSet<>();
		taskIds.add(taskId);
		return getLogEntriesSinceForTasks(taskIds, sinceTimestamp);
	}

	@Override
	public Collection<InvocStat> getLogEntriesSinceForTasks(Set<Long> taskIds,
			long sinceTimestamp) {
		Collection<InvocStat> stats = new LinkedList<>();
		for (Map<Long, InvocStat> invocStats : runToInvocStats.values()) {
			for (InvocStat stat : invocStats.values()) {
				if (taskIds.contains(stat.getTaskId())
						&& stat.getTimestamp() > sinceTimestamp) {
					stats.add(stat);
				}
			}
		}
		return stats;
	}

	@Override
	public Set<Long> getTaskIdsForWorkflow(String workflowName) {
		return workflowNameToTaskIds.containsKey(workflowName) ? workflowNameToTaskIds
				.get(workflowName) : new HashSet<Long>();
	}

	@Override
	public String getTaskName(long taskId) {
		return taskIdToTaskName.get(taskId);
	}

	@Override
	public void logToDB(JsonReportEntry entry) {
		Long invocId = entry.getInvocId();
		UUID runId = entry.getRunId();
		String fileName = entry.getFile();

		if (!runToInvocStats.containsKey(runId)) {
			Map<Long, InvocStat> invocStats = new HashMap<>();
			runToInvocStats.put(runId, invocStats);
		}

		if (invocId != null && !runToInvocStats.get(runId).containsKey(invocId)) {
			InvocStat invocStat = new InvocStat();
			invocStat.setTaskId(entry.getTaskId());
			workflowNameToTaskIds.get(runToWorkflowName.get(runId)).add(
					entry.getTaskId());
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
		}

		try {
			switch (entry.getKey()) {
			case Constant.KEY_WF_NAME:
				runToWorkflowName.put(runId, entry.getValueRawString());
				Set<Long> taskIds = new HashSet<>();
				workflowNameToTaskIds.put(entry.getValueRawString(), taskIds);
				break;
			case JsonReportEntry.KEY_INVOC_TIME:
				invocStat.setRealTime(entry.getValueJsonObj().getDouble(
						"realTime"), entry.getTimestamp());
				break;
			case HiwayDBI.KEY_INVOC_HOST:
				String hostName = entry.getValueRawString();
				invocStat.setHostName(hostName);
				hostNames.add(hostName);
				break;
			case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
				invocStat.getInputFile(fileName).setSize(
						Long.parseLong(entry.getValueRawString()));
				break;
			case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
				invocStat.getOutputFile(fileName).setSize(
						Long.parseLong(entry.getValueRawString()));
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEIN:
				invocStat.getInputFile(fileName).setRealTime(
						(entry.getValueJsonObj().getDouble("realTime")));
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEOUT:
				invocStat.getOutputFile(fileName).setRealTime(
						(entry.getValueJsonObj().getDouble("realTime")));
				break;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
