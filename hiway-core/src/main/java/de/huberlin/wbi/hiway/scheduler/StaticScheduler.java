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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;

import de.huberlin.wbi.hiway.app.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * An abstract implementation of a static workflow scheduler (i.e., a scheduler
 * that build a static schedule of which task to assign to which resource prior
 * to workflow execution).
 * 
 * @author Marc Bux
 * 
 */
public abstract class StaticScheduler extends AbstractScheduler {

	private static final Log log = LogFactory.getLog(StaticScheduler.class);

	// the tasks per compute node that are ready to execute
	protected Map<String, Queue<TaskInstance>> queues;

	// the static schedule
	protected Map<TaskInstance, String> schedule;

	public StaticScheduler(String workflowName, FileSystem fs, HiWayConfiguration conf) {
		super(workflowName, conf);
		schedule = new HashMap<>();
		queues = new HashMap<>();

		parseLogs(fs);

		for (String node : runtimeEstimatesPerNode.keySet()) {
			Queue<TaskInstance> queue = new LinkedList<>();
			queues.put(node, queue);
		}
	}

	@Override
	public void addTaskToQueue(TaskInstance task) {
		String node = schedule.get(task);
		String[] nodes = new String[1];
		nodes[0] = node;
		unissuedNodeRequests.add(nodes);
		queues.get(node).add(task);
		log.info("Added task " + task + " to queue " + node);
	}

	@Override
	public TaskInstance getNextTask(Container container) {
		super.getNextTask(container);
		String node = container.getNodeId().getHost();

		log.info("Looking for task on container " + container.getId().getId()
				+ " on node " + node);
		log.info("Queue: " + queues.get(node).toString());

		TaskInstance task = queues.get(node).remove();

		log.info("Assigned task " + task + " to container "
				+ container.getId().getId() + " on node " + node);
		task.incTries();

		return task;
	}

	@Override
	public int getNumberOfReadyTasks() {
		int readyTasks = 0;
		for (Queue<TaskInstance> queue : queues.values()) {
			readyTasks += queue.size();
		}
		return readyTasks;
	}

	@Override
	public boolean relaxLocality() {
		return false;
	}

}
