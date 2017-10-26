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
package de.huberlin.wbi.hiway.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.json.JSONException;

import de.huberlin.hiwaydb.useDB.HiwayDB;
import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.hiwaydb.useDB.HiwayDBNoSQL;
import de.huberlin.hiwaydb.useDB.InvocStat;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.ProvenanceManager;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * An abstract implementation of a workflow scheduler.
 * 
 * @author Marc Bux
 * 
 */
public abstract class WorkflowScheduler {

	protected HiWayConfiguration conf;
	protected HiwayDBI dbInterface;
	protected FileSystem hdfs;
	protected int maxRetries = 0;
	protected Map<String, Long> maxTimestampPerHost;
	protected int numberOfFinishedTasks = 0;
	protected int numberOfPreviousRunTasks = 0;
	protected int numberOfRemainingTasks = 0;
	protected int numberOfRunningTasks = 0;
	protected boolean relaxLocality = true;
	protected Map<String, Map<Long, WienerProcessModel>> runtimePerBotPerVm;
	protected Set<Long> taskIds;
	// a queue of nodes on which containers are to be requested
	protected Queue<ContainerRequest> unissuedContainerRequests;
	protected String workflowName;
	protected int containerMemory;
	protected Map<String, Integer> customMemoryMap;
	protected int containerCores;
	protected int requestPriority;

	public WorkflowScheduler(String workflowName) {
		this.workflowName = workflowName;

		unissuedContainerRequests = new LinkedList<>();

		taskIds = new HashSet<>();
		runtimePerBotPerVm = new HashMap<>();
		maxTimestampPerHost = new HashMap<>();
	}

	public void init(HiWayConfiguration conf_, FileSystem hdfs_, int containerMemory_, Map<String, Integer> customMemoryMap_, int containerCores_,
	    int requestPriority_) {
		this.conf = conf_;
		this.hdfs = hdfs_;
		this.containerMemory = containerMemory_;
		this.customMemoryMap = customMemoryMap_;
		this.containerCores = containerCores_;
		this.requestPriority = requestPriority_;
	}

	protected ContainerRequest setupContainerAskForRM(String[] nodes, int memory) {
		// set the priority for the request
		Priority pri = Priority.newInstance(requestPriority);
		// pri.setPriority(requestPriority);

		// set up resource type requirements
		Resource capability = Resource.newInstance(memory, containerCores);
		// capability.setMemory(containerMemory);
		// capability.setVirtualCores(containerCores);

		return new ContainerRequest(capability, nodes, null, pri, relaxLocality());
	}

	public void addEntryToDB(JsonReportEntry entry) {
		// WorkflowDriver.writeToStdout("HiwayDB: Adding entry " + entry + " to database.");
		dbInterface.logToDB(entry);
		// WorkflowDriver.writeToStdout("HiwayDB: Added entry to database.");
	}

	protected abstract void addTask(TaskInstance task);

	public void addTasks(Collection<TaskInstance> tasks) {
		for (TaskInstance task : tasks) {
			addTask(task);
		}
	}

	public abstract void addTaskToQueue(TaskInstance task);

	public HiwayDBI getDbInterface() {
		return dbInterface;
	}

	public ContainerRequest getNextNodeRequest() {
		return unissuedContainerRequests.remove();
	}

	public abstract TaskInstance getTask(Container container);

	protected Set<String> getNodeIds() {
		return new HashSet<>(runtimePerBotPerVm.keySet());
	}

	public int getNumberOfFinishedTasks() {
		return numberOfFinishedTasks - numberOfPreviousRunTasks;
	}

	public abstract int getNumberOfReadyTasks();

	public int getNumberOfRunningTasks() {
		return numberOfRunningTasks;
	}

	public int getNumberOfTotalTasks() {
		int fin = getNumberOfFinishedTasks();
		int run = getNumberOfRunningTasks();
		int rem = numberOfRemainingTasks;
		
		return fin + run + rem;
	}

	protected Set<Long> getTaskIds() {
		return new HashSet<>(taskIds);
	}

	public boolean hasNextNodeRequest() {
		return !unissuedContainerRequests.isEmpty();
	}

	public void initializeProvenanceManager() {
		maxRetries = conf.getInt(HiWayConfiguration.HIWAY_AM_TASK_RETRIES, HiWayConfiguration.HIWAY_AM_TASK_RETRIES_DEFAULT);

		HiWayConfiguration.HIWAY_DB_TYPE_OPTS dbType = HiWayConfiguration.HIWAY_DB_TYPE_OPTS.valueOf(conf.get(HiWayConfiguration.HIWAY_DB_TYPE,
		    HiWayConfiguration.HIWAY_DB_TYPE_DEFAULT.toString()));
		switch (dbType) {
		case SQL:
			String sqlUser = conf.get(HiWayConfiguration.HIWAY_DB_SQL_USER);
			if (sqlUser == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_USER + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			String sqlPassword = conf.get(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD);
			if (sqlPassword == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			String sqlURL = conf.get(HiWayConfiguration.HIWAY_DB_SQL_URL);
			if (sqlURL == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_URL + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			dbInterface = new HiwayDB(sqlUser, sqlPassword, sqlURL);
			break;
		case NoSQL:
			sqlUser = conf.get(HiWayConfiguration.HIWAY_DB_SQL_USER);
			if (sqlUser == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_USER + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			sqlPassword = conf.get(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD);
			if (sqlPassword == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_PASSWORD + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			sqlURL = conf.get(HiWayConfiguration.HIWAY_DB_SQL_URL);
			if (sqlURL == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_SQL_URL + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			String noSqlBucket = conf.get(HiWayConfiguration.HIWAY_DB_NOSQL_BUCKET);
			if (noSqlBucket == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_NOSQL_BUCKET + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			String noSqlPassword = conf.get(HiWayConfiguration.HIWAY_DB_NOSQL_PASSWORD);
			if (noSqlPassword == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_NOSQL_PASSWORD + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			String noSqlURIs = conf.get(HiWayConfiguration.HIWAY_DB_NOSQL_URLS);
			if (noSqlURIs == null) {
				WorkflowDriver.writeToStdout(HiWayConfiguration.HIWAY_DB_NOSQL_URLS + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
				throw new RuntimeException();
			}
			List<URI> noSqlURIList = new ArrayList<>();
			for (String uri : noSqlURIs.split(",")) {
				noSqlURIList.add(URI.create(uri));
			}
			dbInterface = new HiwayDBNoSQL(noSqlBucket, noSqlPassword, noSqlURIList, sqlUser, sqlPassword, sqlURL);

			break;
		default:
			dbInterface = new ProvenanceManager();
			parseLogs();
		}
	}

	protected void newHost(String nodeId) {
		Map<Long, WienerProcessModel> runtimeEstimates = new HashMap<>();
		for (long taskId : getTaskIds()) {
			runtimeEstimates.put(taskId, new WienerProcessModel(taskId, nodeId, false));
		}
		runtimePerBotPerVm.put(nodeId, runtimeEstimates);
		maxTimestampPerHost.put(nodeId, 0l);
	}

	protected void newTask(long taskId) {
		taskIds.add(taskId);
		for (Entry<String, Map<Long, WienerProcessModel>> runtimeEstimates : runtimePerBotPerVm.entrySet()) {
			runtimeEstimates.getValue().put(taskId, new WienerProcessModel(taskId, runtimeEstimates.getKey(), false));
		}
	}

	public boolean nothingToSchedule() {
		return getNumberOfReadyTasks() == 0;
	}

	protected void parseLogs() {
		String hdfsBaseDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE, HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE_DEFAULT);
		String hdfsSandboxDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE, HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE_DEFAULT);
		Path hdfsBaseDirectory = new Path(new Path(hdfs.getUri()), hdfsBaseDirectoryName);
		Path hdfsSandboxDirectory = new Path(hdfsBaseDirectory, hdfsSandboxDirectoryName);
		try {
			for (FileStatus appDirStatus : hdfs.listStatus(hdfsSandboxDirectory)) {
				if (appDirStatus.isDirectory()) {
					Path appDir = appDirStatus.getPath();
					for (FileStatus srcStatus : hdfs.listStatus(appDir)) {
						Path src = srcStatus.getPath();
						String srcName = src.getName();
						if (srcName.endsWith(".log")) {
							Path dest = new Path(appDir.getName());
							WorkflowDriver.writeToStdout("Parsing log " + dest.toString());
							hdfs.copyToLocalFile(false, src, dest);

							try (BufferedReader reader = new BufferedReader(new FileReader(new File(dest.toString())))) {
								String line;
								while ((line = reader.readLine()) != null) {
									if (line.length() == 0)
										continue;
									JsonReportEntry entry = new JsonReportEntry(line);
									addEntryToDB(entry);
								}
							}
						}
					}
				}
			}
		} catch (IOException | JSONException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
	}

	public boolean relaxLocality() {
		return relaxLocality;
	}

	public Collection<ContainerId> taskCompleted(TaskInstance task, ContainerStatus containerStatus, long runtimeInMs) {

		numberOfRunningTasks--;
		numberOfFinishedTasks++;

		WorkflowDriver.writeToStdout("Task " + task + " on container " + containerStatus.getContainerId() + " completed successfully after " + runtimeInMs + " ms");

		return new ArrayList<>();
	}

	public Collection<ContainerId> taskFailed(TaskInstance task, ContainerStatus containerStatus) {
		numberOfRunningTasks--;

		WorkflowDriver.writeToStdout("Task " + task + " on container " + containerStatus.getContainerId() + " failed");
		if (task.retry(maxRetries)) {
			WorkflowDriver.writeToStdout("Retrying task " + task + ".");
			addTask(task);
		} else {
			WorkflowDriver.writeToStdout("Task " + task + " has exceeded maximum number of allowed retries. Aborting workflow.");
			System.exit(-1);
		}

		return new ArrayList<>();
	}

	protected void updateRuntimeEstimate(InvocStat stat) {
		WienerProcessModel re = runtimePerBotPerVm.get(stat.getHostName()).get(stat.getTaskId());
		re.addRuntime(stat.getTimestamp(), stat.getRealTime());
	}

	public void updateRuntimeEstimates(String runId) {
		if (HiWayConfiguration.verbose)
			WorkflowDriver.writeToStdout("Updating Runtime Estimates.");

		// WorkflowDriver.writeToStdout("HiwayDB: Querying Host Names from database.");
		Collection<String> newHostIds = dbInterface.getHostNames();
		// WorkflowDriver.writeToStdout("HiwayDB: Retrieved Host Names " + newHostIds.toString() + " from database.");
		newHostIds.removeAll(getNodeIds());
		for (String newHostId : newHostIds) {
			newHost(newHostId);
		}
		// WorkflowDriver.writeToStdout("HiwayDB: Querying Task Ids for workflow " + workflowName + " from database.");
		Collection<Long> newTaskIds = dbInterface.getTaskIdsForWorkflow(workflowName);
		// WorkflowDriver.writeToStdout("HiwayDB: Retrieved Task Ids " + newTaskIds.toString() + " from database.");

		newTaskIds.removeAll(getTaskIds());
		for (long newTaskId : newTaskIds) {
			newTask(newTaskId);
		}

		for (String hostName : getNodeIds()) {
			long oldMaxTimestamp = maxTimestampPerHost.get(hostName);
			long newMaxTimestamp = oldMaxTimestamp;
			for (long taskId : getTaskIds()) {
				// WorkflowDriver.writeToStdout("HiwayDB: Querying InvocStats for task id " + taskId + " on host " + hostName + " since timestamp " + oldMaxTimestamp
				// + " from database.");
				Collection<InvocStat> invocStats = dbInterface.getLogEntriesForTaskOnHostSince(taskId, hostName, oldMaxTimestamp);
				// WorkflowDriver.writeToStdout("HiwayDB: Retrieved InvocStats " + invocStats.toString() + " from database.");
				for (InvocStat stat : invocStats) {
					newMaxTimestamp = Math.max(newMaxTimestamp, stat.getTimestamp());
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
