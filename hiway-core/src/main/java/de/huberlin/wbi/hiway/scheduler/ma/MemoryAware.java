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
package de.huberlin.wbi.hiway.scheduler.ma;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.yarn.api.records.Container;

import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.scheduler.WorkflowScheduler;

/**
 * A basic implementation of a memory-aware greedy queue.
 * 
 * @author Marc Bux
 * 
 */
public class MemoryAware extends WorkflowScheduler {

	private Map<Integer, Queue<TaskInstance>> queuePerMem;

	public MemoryAware(String workflowName) {
		super(workflowName);
		queuePerMem = new HashMap<>();
	}

	@Override
	protected void addTask(TaskInstance task) {
		numberOfRemainingTasks++;
		if (task.readyToExecute())
			addTaskToQueue(task);
	}

	@Override
	public void addTaskToQueue(TaskInstance task) {
		int memory = customMemoryMap.containsKey(task.getTaskName()) ? customMemoryMap.get(task.getTaskName()) : containerMemory;
		System.out.println("Adding task " + task + " to queue " + memory);
		unissuedContainerRequests.add(setupContainerAskForRM(new String[0], memory));

		if (!queuePerMem.containsKey(memory)) {
			queuePerMem.put(memory, new LinkedList<TaskInstance>());
		}
		queuePerMem.get(memory).add(task);
	}

	@Override
	public TaskInstance getTask(Container container) {
		System.out.println("Looking for task to place in container " + container.getId() + "@" + container.getNodeId().getHost() + " ("
				+ container.getResource().getVirtualCores() + ", " + container.getResource().getMemory() + ").");
		numberOfRemainingTasks--;
		numberOfRunningTasks++;

		int memory = container.getResource().getMemory();
		Queue<TaskInstance> queue = queuePerMem.get(memory);
		TaskInstance task = queue.remove();

		System.out.println("Found task " + task + ".");

		task.incTries();
		return task;
	}

	@Override
	public int getNumberOfReadyTasks() {
		int nReadyTasks = 0;
		for (Queue<TaskInstance> queue : queuePerMem.values())
			nReadyTasks += queue.size();
		return nReadyTasks;
	}

}
