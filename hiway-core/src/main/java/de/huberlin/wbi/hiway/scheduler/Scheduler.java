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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * The interface implemented by schedulers in Hi-WAY.
 * 
 * @author Marc Bux
 * 
 */
public interface Scheduler {
	
	public void initialize();
	
	public void updateRuntimeEstimates(String runId);

	public void addEntryToDB(JsonReportEntry entry);

	// protected void addTask(TaskInstance task);

	public void addTasks(Collection<TaskInstance> tasks);

	// all schedulers have an internal queue in which they store tasks that are
	// ready to execute
	public void addTaskToQueue(TaskInstance task);

	public String[] getNextNodeRequest();

	public TaskInstance getNextTask(Container container);

	public int getNumberOfFinishedTasks();

	public int getNumberOfReadyTasks();

	public int getNumberOfRunningTasks();

	public int getNumberOfTotalTasks();

	public boolean hasNextNodeRequest();

	public boolean nothingToSchedule();

	// determines whether container requests are tied to specific worker nodes
	public boolean relaxLocality();

	public Collection<ContainerId> taskCompleted(TaskInstance task,
			ContainerStatus containerStatus, long runtimeInMs);

	public Collection<ContainerId> taskFailed(TaskInstance task,
			ContainerStatus containerStatus);

	// public void addTaskToQueue(TaskInstance task);

}
