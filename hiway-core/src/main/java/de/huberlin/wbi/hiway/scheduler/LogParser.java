package de.huberlin.wbi.hiway.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.Constant;
import de.huberlin.wbi.hiway.common.FileStat;
import de.huberlin.wbi.hiway.common.HiwayDBI;
import de.huberlin.wbi.hiway.common.InvocStat;

public class LogParser implements HiwayDBI {

	private Map<UUID, String> runToWorkflowName;
	private Map<String, Set<Long>> workflowNameToTaskIds;
	private Set<String> hostNames;
	private Map<Long, InvocStat> invocStats;

	public LogParser() {
		runToWorkflowName = new HashMap<>();
		workflowNameToTaskIds = new HashMap<>();
		hostNames = new HashSet<>();
		invocStats = new HashMap<>();
	}
	
	@Override
	public Set<String> getHostNames() {
		return hostNames;
	}

	@Override
	public Set<Long> getTaskIdsForWorkflow(String workflowName) {
		return workflowNameToTaskIds.get(workflowName);
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
		for (InvocStat stat : invocStats.values()) {
			if (taskIds.contains(stat.getTaskId())
					&& stat.getTimestamp() > sinceTimestamp) {
				stats.add(stat);
			}
		}
		return stats;
	}

	@Override
	public void logToDB(JsonReportEntry entry) {
		Long invocId = entry.getInvocId();
		String fileName = entry.getFile();

		if (invocId != null && !invocStats.containsKey(invocId)) {
			InvocStat invocStat = new InvocStat(entry.getTimestamp(),
					entry.getTaskId());
			workflowNameToTaskIds.get(runToWorkflowName.get(entry.getRunId())).add(entry.getTaskId());
			invocStats.put(invocId, invocStat);
		}
		InvocStat invocStat = invocStats.get(invocId);

		switch (entry.getKey()) {
		case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
		case Constant.KEY_FILE_TIME_STAGEIN:
			if (!invocStat.containsInputFile(fileName)) {
				FileStat fileStat = new FileStat(fileName);
				invocStat.addInputFile(fileStat);
			}
			break;
		case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
		case Constant.KEY_FILE_TIME_STAGEOUT:
			if (!invocStat.containsOutputFile(fileName)) {
				FileStat fileStat = new FileStat(fileName);
				invocStat.addOutputFile(fileStat);
			}
		}

		try {
			switch (entry.getKey()) {
			case Constant.KEY_WF_NAME:
				runToWorkflowName.put(entry.getRunId(), entry.getValue());
				break;
			case JsonReportEntry.KEY_INVOC_TIME:
				invocStat.setRealTime(entry.getValueJsonObj().getLong(
						"realTime"));
				break;
			case Constant.KEY_INVOC_HOST:
				String hostName = entry.getValueRawString();
				invocStat.setHostname(hostName);
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
			case Constant.KEY_FILE_TIME_STAGEIN:
				invocStat.getInputFile(fileName).setRealTime(
						(entry.getValueJsonObj().getLong("realTime")));
				break;
			case Constant.KEY_FILE_TIME_STAGEOUT:
				invocStat.getOutputFile(fileName).setRealTime(
						(entry.getValueJsonObj().getLong("realTime")));
				break;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
