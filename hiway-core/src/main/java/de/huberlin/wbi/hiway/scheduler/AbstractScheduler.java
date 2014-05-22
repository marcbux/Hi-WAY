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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * An abstract implementation of a workflow scheduler.
 * 
 * @author Marc Bux
 * 
 */
public abstract class AbstractScheduler implements Scheduler {

	private static final Log log = LogFactory.getLog(AbstractScheduler.class);

	private int numberOfFinishedTasks = 0;
	private int numberOfRemainingTasks = 0;
	private int numberOfRunningTasks = 0;

	// a queue of nodes on which containers are to be requested
	protected Queue<String[]> unissuedNodeRequests;

	public AbstractScheduler() {
		// statistics = new HashMap<Long, Map<String, Set<InvocStat>>>();
		unissuedNodeRequests = new LinkedList<>();
	}

	@Override
	public void addTask(TaskInstance task) {
		numberOfRemainingTasks++;
	}

	@Override
	public void addTasks(Collection<TaskInstance> tasks) {
		for (TaskInstance task : tasks) {
			addTask(task);
		}
	}

	// all schedulers have an internal queue in which they store tasks that are ready to execute
	protected void addTaskToQueue(TaskInstance task) {
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

	@Override
	public int getNumberOfFinishedTasks() {
		return numberOfFinishedTasks;
	}

	@Override
	public int getNumberOfRunningTasks() {
		return numberOfRunningTasks;
	}

	@Override
	public int getNumberOfTotalTasks() {
		log.debug("Update on total tasks:");
		int finishedTasks = getNumberOfFinishedTasks();
		log.debug("\tfinished:  " + finishedTasks);
		int runningTasks = getNumberOfRunningTasks();
		log.debug("\trunning:   " + runningTasks);
		log.debug("\tremaining: " + numberOfRemainingTasks);
		return finishedTasks + runningTasks + numberOfRemainingTasks;
	}

	@Override
	public boolean hasNextNodeRequest() {
		return !unissuedNodeRequests.isEmpty();
	}

	@Override
	public boolean nothingToSchedule() {
		return getNumberOfReadyTasks() == 0;
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
		if (task.retry()) {
			log.info("Retrying task " + task + ".");
			addTask(task);
		} else {
			log.info("Task "
					+ task
					+ " has exceeded maximum number of allowed retries. Aborting task.");
		}

		return new ArrayList<>();
	}

}
