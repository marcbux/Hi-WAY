package de.huberlin.wbi.hiway.common;

import java.util.Collection;
import java.util.Set;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public interface HiwayDBI {
	
	public void logToDB(JsonReportEntry entry);
	
	public Set<Long> getTaskIds();
	public Set<String> getHostNames();
	
	public Collection<InvocStat> getLogEntries();
	public Collection<InvocStat> getLogEntriesForTask (long taskId );
	public Collection<InvocStat> getLogEntriesForTasks (Set<Long> taskIds );
	public Collection<InvocStat> getLogEntriesSince(long sinceTimestamp);
	public Collection<InvocStat> getLogEntriesSinceForTask (long taskId, long sinceTimestamp );
	public Collection<InvocStat> getLogEntriesSinceForTasks (Set<Long> taskIds, long sinceTimestamp );
	
}
