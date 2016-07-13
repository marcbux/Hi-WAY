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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

import de.huberlin.wbi.hiway.common.HiWayConfiguration;
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

	@SuppressWarnings("rawtypes")
	private AMRMClientAsync amRMClient;
	private int maxMem;
	private int maxCores;

	@SuppressWarnings("rawtypes")
	public MemoryAware(String workflowName, AMRMClientAsync amRMClient) {
		super(workflowName);
		queuePerMem = new HashMap<>();
		this.amRMClient = amRMClient;
	}

	@Override
	public void init(HiWayConfiguration conf_, FileSystem hdfs_, int containerMemory_, Map<String, Integer> customMemoryMap_, int containerCores_,
			int requestPriority_) {
		super.init(conf_, hdfs_, containerMemory_, customMemoryMap_, containerCores_, requestPriority_);
		maxMem = conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
		maxCores = conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
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
		numberOfRemainingTasks--;
		numberOfRunningTasks++;

		int memory = container.getResource().getMemory();
		Queue<TaskInstance> queue = queuePerMem.get(memory);
		TaskInstance task = queue.remove();

		System.out.println("Assigned task " + task + " to container " + container.getId() + "@" + container.getNodeId().getHost() + ":"
				+ container.getResource().getVirtualCores() + ":" + container.getResource().getMemory());

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

	@SuppressWarnings("unchecked")
	@Override
	public boolean hasNextNodeRequest() {
		boolean hasNextNodeRequest = super.hasNextNodeRequest();
		// Hadoop has a weird bug where open container requests are sometimes "forgotten" (i.e., not fulfilled despite available resources).
		// Running into this bug can be prevented by occasionally re-issuing ContainerRequests.
		if (!hasNextNodeRequest) {
			List<? extends Collection<ContainerRequest>> requestCollections = amRMClient.getMatchingRequests(Priority.newInstance(requestPriority),
					ResourceRequest.ANY, Resource.newInstance(maxMem, maxCores));
			for (Collection<ContainerRequest> requestCollection : requestCollections) {
				ContainerRequest first = requestCollection.iterator().next();
				amRMClient.removeContainerRequest(first);
				amRMClient.addContainerRequest(first);
			}
		}
		return hasNextNodeRequest;
	}

}
