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

//import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

//import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.Test;

//import de.huberlin.wbi.hiway.app.ApplicationMaster;
//import de.huberlin.wbi.hiway.app.DaxApplicationMaster;
import de.huberlin.wbi.hiway.common.AbstractTaskInstance;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.scheduler.C3PO;
import de.huberlin.wbi.hiway.scheduler.GreedyQueue;
import de.huberlin.wbi.hiway.scheduler.Scheduler;

public class TestScheduler {

	private void run(Scheduler scheduler, List<String> nodeNames,
			List<String> taskNames, int[][] runtimes) {

		Queue<NodeId> availableNodes = new LinkedList<>();
		for (String nodeName : nodeNames) {
			NodeId node = NodeId.newInstance(nodeName, 0);
			availableNodes.add(node);
		}

		Map<Container, TaskInstance> runningTasks = new HashMap<>();
		Map<Container, Long> finishTimes = new HashMap<>();

		int runningId = 0;
		long clock = 0;
		while (!scheduler.nothingToSchedule() || !runningTasks.isEmpty()) {
			if (!scheduler.nothingToSchedule() && !availableNodes.isEmpty()) {
				NodeId nodeId = availableNodes.remove();
				ContainerId containerId = ContainerId.newInstance(null,
						runningId++);
				Container container = Container.newInstance(containerId,
						nodeId, "", null, null, null);
				TaskInstance task = scheduler.getNextTask(container);
				runningTasks.put(container, task);
				long runtime = (runtimes == null) ? 1 : runtimes[nodeNames
						.indexOf(nodeId.getHost())][taskNames.indexOf(task
						.getTaskName())];

				finishTimes.put(container, clock + runtime);
			}

			for (Container container : finishTimes.keySet()) {
				if (clock == finishTimes.get(container)) {
					NodeId nodeId = container.getNodeId();
					ContainerStatus containerStatus = ContainerStatus
							.newInstance(container.getId(), null, "", 0);
					TaskInstance task = runningTasks.get(container);
					task.setCompleted();
					long runtime = (runtimes == null) ? 1 : runtimes[nodeNames
							.indexOf(nodeId.getHost())][taskNames.indexOf(task
							.getTaskName())];
					scheduler.taskCompleted(task, containerStatus, runtime);
					runningTasks.remove(container);
					availableNodes.add(nodeId);
				}
			}

			clock++;
		}
	}

	private void shakeRattelRoll(Scheduler scheduler) {

		List<String> nodeNames = new ArrayList<>();
		nodeNames.add("Charlie");
		nodeNames.add("Tango");
		nodeNames.add("Foxtrot");
		List<String> taskNames = new ArrayList<>();
		taskNames.add("Shake");
		taskNames.add("Rattle");
		taskNames.add("Roll");
		int[] nTasks = { 50, 100, 30 };
		int[][] runtimes = { { 5, 10, 20 }, { 10, 5, 50 }, { 15, 5, 30 } };

		List<TaskInstance> tasks = new ArrayList<>();
		for (String taskName : taskNames) {
			for (int i = 0; i < nTasks[taskNames.indexOf(taskName)]; i++)
				tasks.add(new AbstractTaskInstance(UUID.randomUUID(), taskName,
						taskName.hashCode()));
		}

		scheduler.addTasks(tasks);

		run(scheduler, nodeNames, taskNames, runtimes);

	}

	// @Test
	// public void montage05GreedyQueue() {
	// montage05("greedyQueue");
	// }

	// @Test
	// public void montage05C3PO() {
	// montage05("c3po");
	// }

	// private void montage05(String scheduler) {
	// List<String> nodeNames = new ArrayList<>();
	// nodeNames.add("Worker");
	// ApplicationMaster workflow;
	// try {
	// workflow = new DaxApplicationMaster();
	// String[] args = { "--workflow", "examples/dag_0.5.xml",
	// "--scheduler", scheduler };
	// workflow.init(args);
	// workflow.parseWorkflow();
	//
	// run(workflow.getScheduler(), nodeNames, null, null);
	// } catch (IOException e1) {
	// e1.printStackTrace();
	// } catch (ParseException e) {
	// e.printStackTrace();
	// }
	// }

	@Test
	public void shakeRattelRollC3PO() {
		C3PO c3po = new C3PO("shakeRattleRoll", 0, null);
		c3po.setnClones(0);
		shakeRattelRoll(c3po);
	}

	@Test
	public void shakeRattelRollGreedyQueue() {
		shakeRattelRoll(new GreedyQueue("shakeRattleRoll", null, null));
	}

}
