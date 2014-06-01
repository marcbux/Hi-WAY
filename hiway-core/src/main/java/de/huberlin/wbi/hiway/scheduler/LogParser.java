package de.huberlin.wbi.hiway.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.Constant;
import de.huberlin.wbi.hiway.common.FileStat;
import de.huberlin.wbi.hiway.common.HiwayDBI;
import de.huberlin.wbi.hiway.common.InvocStat;

public class LogParser implements HiwayDBI {

	Map<Long, InvocStat> invocStats;

	public LogParser() {
		invocStats = new HashMap<>();
	}

	@Override
	public Set<String> getHostNames() {
		Set<String> hostNames = new HashSet<>();
		for (InvocStat stat : invocStats.values()) {
			String hostName = stat.getHostname();
			if (!hostNames.contains(hostName)) {
				hostNames.add(hostName);
			}
		}
		return hostNames;
	}

	@Override
	public Set<Long> getTaskIds() {
		Set<Long> taskIds = new HashSet<>();
		for (InvocStat stat : invocStats.values()) {
			long taskId = stat.getTaskId();
			if (!taskIds.contains(taskId)) {
				taskIds.add(taskId);
			}
		}
		return taskIds;
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
		return getLogEntriesSinceForTasks(getTaskIds(), 0l);
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
			case JsonReportEntry.KEY_INVOC_TIME:
				invocStat.setRealTime(entry.getValueJsonObj().getLong(
						"realTime"));
				break;
			case Constant.KEY_INVOC_HOST:
				invocStat.setHostname(entry.getValueRawString());
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
