package de.huberlin.wbi.hiway.common;

import java.util.Collection;
import java.util.Set;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public interface HiwayDBI {

	public Set<String> getHostNames();

	public Collection<InvocStat> getLogEntries();

	public Collection<InvocStat> getLogEntriesForTask(long taskId);

	public Collection<InvocStat> getLogEntriesForTasks(Set<Long> taskIds);

	public Collection<InvocStat> getLogEntriesSince(long sinceTimestamp);

	public Collection<InvocStat> getLogEntriesSinceForTask(long taskId,
			long sinceTimestamp);

	public Collection<InvocStat> getLogEntriesSinceForTasks(Set<Long> taskIds,
			long sinceTimestamp);

	public Set<Long> getTaskIdsForWorkflow(String workflowName);

	public String getTaskName(long taskId);

	public void logToDB(JsonReportEntry entry);

}
