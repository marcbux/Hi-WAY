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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import de.huberlin.hiwaydb.useDB.HiwayDB;
import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.hiwaydb.useDB.HiwayDBNoSQL;
import de.huberlin.hiwaydb.useDB.InvocStat;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.app.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
//import de.huberlin.wbi.hiway.scheduler.C3PO.ConservatismEstimate;
//import de.huberlin.wbi.hiway.scheduler.C3PO.Estimate;

/**
 * An abstract implementation of a workflow scheduler.
 * 
 * @author Marc Bux
 * 
 */
public abstract class AbstractScheduler implements Scheduler {

	protected class Estimate {
		String taskName;
		double weight = 1d;
	}

	protected class RuntimeEstimate extends Estimate {
		long averageRuntime;
		int finishedTasks;
		// int remainingTasks;
		// int runningTasks;
		long timeSpent;
	}

	private static final Log log = LogFactory.getLog(AbstractScheduler.class);

	// node -> task -> invocstats
	// protected Map<String, Map<String, Set<InvocStat>>> invocStats;
	protected HiwayDBI dbInterface;

	protected Map<String, Long> maxTimestampPerHost;

	protected int numberOfPreviousRunTasks = 0;
	protected int numberOfFinishedTasks = 0;
	protected int numberOfRemainingTasks = 0;
	protected int numberOfRunningTasks = 0;

	protected final FileSystem fs;

	protected int maxRetries = 0;

	// private Set<String> nodeIds;
	protected Set<Long> taskIds;
	protected Map<String, Map<Long, RuntimeEstimate>> runtimeEstimatesPerNode;

	// protected Map<String, Map<Long, Long>> totalRuntimes;
	// protected Map<String, Map<Long, Long>> nExecutions;
	// protected Map<String, Map<Long, Double>> runtimeEstimates;

	// a queue of nodes on which containers are to be requested
	protected Queue<String[]> unissuedNodeRequests;
	protected String workflowName;
	protected HiWayConfiguration hiWayConf;

	public AbstractScheduler(String workflowName, HiWayConfiguration conf,
			FileSystem fs) {
		// statistics = new HashMap<Long, Map<String, Set<InvocStat>>>();
		this.workflowName = workflowName;

		this.hiWayConf = conf;
		this.fs = fs;
		unissuedNodeRequests = new LinkedList<>();

		taskIds = new HashSet<>();
		runtimeEstimatesPerNode = new HashMap<>();
		maxTimestampPerHost = new HashMap<>();
	}

	@Override
	public void initialize() {
		maxRetries = hiWayConf.getInt(HiWayConfiguration.HIWAY_AM_TASK_RETRIES,
				HiWayConfiguration.HIWAY_AM_TASK_RETRIES_DEFAULT);

		HiWayConfiguration.HIWAY_DB_TYPE_OPTS dbType = HiWayConfiguration.HIWAY_DB_TYPE_OPTS
				.valueOf(hiWayConf.get(HiWayConfiguration.HIWAY_DB_TYPE,
						HiWayConfiguration.HIWAY_DB_TYPE_DEFAULT.toString()));
		switch (dbType) {
		case SQL:
			String sqlUser = hiWayConf
					.get(HiWayConfiguration.HIWAY_DB_SQL_USER);
			String sqlPassword = hiWayConf
					.get(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD);
			String sqlURL = hiWayConf.get(HiWayConfiguration.HIWAY_DB_SQL_URL);
			dbInterface = new HiwayDB(sqlUser, sqlPassword, sqlURL);
			break;
		case NoSQL:
			sqlUser = hiWayConf.get(HiWayConfiguration.HIWAY_DB_SQL_USER);
			sqlPassword = hiWayConf
					.get(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD);
			sqlURL = hiWayConf.get(HiWayConfiguration.HIWAY_DB_SQL_URL);
			String noSqlBucket = hiWayConf
					.get(HiWayConfiguration.HIWAY_DB_NOSQL_BUCKET);
			String noSqlPassword = hiWayConf
					.get(HiWayConfiguration.HIWAY_DB_NOSQL_PASSWORD);
			String noSqlURIs = hiWayConf
					.get(HiWayConfiguration.HIWAY_DB_NOSQL_URLS);
			List<URI> noSqlURIList = new ArrayList<>();
			for (String uri : noSqlURIs.split(",")) {
				noSqlURIList.add(URI.create(uri));
			}
			dbInterface = new HiwayDBNoSQL(noSqlBucket, noSqlPassword,
					noSqlURIList, sqlUser, sqlPassword, sqlURL);
			break;
		default:
			dbInterface = new LogParser();
			parseLogs(fs);
		}
	}

	@Override
	public void addEntryToDB(JsonReportEntry entry) {
		log.info("HiwayDB: Adding entry " + entry + " to database.");
		dbInterface.logToDB(entry);
	}

	protected void addTask(TaskInstance task) {
		numberOfRemainingTasks++;
	}

	@Override
	public void addTasks(Collection<TaskInstance> tasks) {
		for (TaskInstance task : tasks) {
			addTask(task);
		}
	}

	@Override
	public void addTaskToQueue(TaskInstance task) {
		unissuedNodeRequests.add(new String[0]);
	}

	@Override
	public String[] getNextNodeRequest() {
		return unissuedNodeRequests.remove();
	}

	@Override
	public TaskInstance getNextTask(Container container) {
		numberOfRemainingTasks--;
		numberOfRunningTasks++;
		return null;
	}

	protected Set<String> getNodeIds() {
		return new HashSet<>(runtimeEstimatesPerNode.keySet());
	}

	@Override
	public int getNumberOfFinishedTasks() {
		return numberOfFinishedTasks - numberOfPreviousRunTasks;
	}

	@Override
	public int getNumberOfRunningTasks() {
		return numberOfRunningTasks;
	}

	@Override
	public int getNumberOfTotalTasks() {
		int fin = getNumberOfFinishedTasks();
		int run = getNumberOfRunningTasks();
		int rem = numberOfRemainingTasks;

		log.info("Scheduled Containers Finished: " + fin);
		log.info("Scheduled Containers Running: " + run);
		log.info("Scheduled Containers Remaining: " + rem);

		return fin + run + rem;
	}

	protected Set<Long> getTaskIds() {
		return new HashSet<>(taskIds);
	}

	@Override
	public boolean hasNextNodeRequest() {
		return !unissuedNodeRequests.isEmpty();
	}

	protected void newHost(String nodeId) {
		Map<Long, RuntimeEstimate> runtimeEstimates = new HashMap<>();
		for (long taskId : getTaskIds()) {
			runtimeEstimates.put(taskId, new RuntimeEstimate());
		}
		runtimeEstimatesPerNode.put(nodeId, runtimeEstimates);
		maxTimestampPerHost.put(nodeId, 0l);
	}

	protected void newTask(long taskId) {
		taskIds.add(taskId);
		for (Map<Long, RuntimeEstimate> runtimeEstimates : runtimeEstimatesPerNode
				.values()) {
			runtimeEstimates.put(taskId, new RuntimeEstimate());
		}
	}

	@Override
	public boolean nothingToSchedule() {
		return getNumberOfReadyTasks() == 0;
	}

	protected void parseLogs(FileSystem fs) {
		Path hiwayDir = new Path(fs.getHomeDirectory(), hiWayConf.get(
				HiWayConfiguration.HIWAY_AM_SANDBOX_DIRECTORY,
				HiWayConfiguration.HIWAY_AM_SANDBOX_DIRECTORY_DEFAULT));
		try {
			for (FileStatus appDirStatus : fs.listStatus(hiwayDir)) {
				if (appDirStatus.isDirectory()) {
					Path appDir = appDirStatus.getPath();
					for (FileStatus srcStatus : fs.listStatus(appDir)) {
						Path src = srcStatus.getPath();
						String srcName = src.getName();
						if (srcName.equals(hiWayConf.get(
								HiWayConfiguration.HIWAY_DB_STAT_LOG,
								HiWayConfiguration.HIWAY_DB_STAT_LOG_DEFAULT))) {
							Path dest = new Path(appDir.getName());
							log.info("Parsing log " + dest.toString());
							fs.copyToLocalFile(false, src, dest);

							try (BufferedReader reader = new BufferedReader(
									new FileReader(new File(dest.toString())))) {
								String line;
								while ((line = reader.readLine()) != null) {
									JsonReportEntry entry = new JsonReportEntry(
											line);
									addEntryToDB(entry);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean relaxLocality() {
		return true;
	}

	@Override
	public Collection<ContainerId> taskCompleted(TaskInstance task,
			ContainerStatus containerStatus, long runtimeInMs) {

		numberOfRunningTasks--;
		numberOfFinishedTasks++;

		log.info("Task " + task + " in container "
				+ containerStatus.getContainerId().getId() + " finished after "
				+ runtimeInMs + " ms");

		return new ArrayList<>();
	}

	@Override
	public Collection<ContainerId> taskFailed(TaskInstance task,
			ContainerStatus containerStatus) {
		numberOfRunningTasks--;

		log.info("Task " + task + " on container "
				+ containerStatus.getContainerId().getId() + " failed");
		if (task.retry(maxRetries)) {
			log.info("Retrying task " + task + ".");
			addTask(task);
		} else {
			log.info("Task "
					+ task
					+ " has exceeded maximum number of allowed retries. Aborting task.");
		}

		return new ArrayList<>();
	}

	protected void updateRuntimeEstimate(InvocStat stat) {
		log.debug("Updating Runtime Estimate for stat " + stat.toString());
		RuntimeEstimate re = runtimeEstimatesPerNode.get(stat.getHostName())
				.get(stat.getTaskId());
		re.finishedTasks += 1;
		re.timeSpent += stat.getRealTime().longValue();
		re.weight = re.averageRuntime = re.timeSpent / re.finishedTasks;
	}

	@Override
	public void updateRuntimeEstimates(String runId) {
		log.info("Updating Runtime Estimates.");

		Collection<String> newHostIds = dbInterface.getHostNames();
		log.info("HiwayDB: Retrieved Host Names " + newHostIds.toString()
				+ " from database.");
		newHostIds.removeAll(getNodeIds());
		for (String newHostId : newHostIds) {
			newHost(newHostId);
		}
		Collection<Long> newTaskIds = dbInterface
				.getTaskIdsForWorkflow(workflowName);
		log.info("HiwayDB: Retrieved Task Ids " + newTaskIds.toString()
				+ " from database.");

		newTaskIds.removeAll(getTaskIds());
		for (long newTaskId : newTaskIds) {
			newTask(newTaskId);
		}

		for (String hostName : getNodeIds()) {
			long oldMaxTimestamp = maxTimestampPerHost.get(hostName);
			long newMaxTimestamp = oldMaxTimestamp;
			for (long taskId : getTaskIds()) {
				log.info("HiwayDB: Querying InvocStats for task id " + taskId
						+ " on host " + hostName + " since timestamp "
						+ oldMaxTimestamp + " from database.");
				for (InvocStat stat : dbInterface
						.getLogEntriesForTaskOnHostSince(taskId, hostName,
								oldMaxTimestamp)) {
					log.info("HiwayDB: Retrieved InvocStat " + stat.toString()
							+ " from database.");
					newMaxTimestamp = Math.max(newMaxTimestamp,
							stat.getTimestamp());
					updateRuntimeEstimate(stat);
					if (!runId.equals(stat.getRunId())) {
						numberOfPreviousRunTasks++;
						numberOfFinishedTasks++;
					}
				}
			}
			maxTimestampPerHost.put(hostName, newMaxTimestamp);
		}
	}
}
