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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.huberlin.wbi.hiway.common.StaticTaskInstance;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

/**
 * <p>
 * The HEFT scheduler, as described in [1]. This implementation of HEFT does not yet make use of estimates for data
 * transfer times between nodes.
 * </p>
 * 
 * <p>
 * [1] Topcuoglu, H., Hariri, S., & Wu, M.-Y. (2002). <i>Performance-Effective and Low-Complexity Task Scheduling for
 * Heterogeneous Computing.</i> IEEE Transactions on Parallel and Distributed Systems, 13(3), 260�274.
 * </p>
 * 
 * @author Marc Bux
 * 
 */
public class HEFT extends StaticScheduler {

	private static final Log log = LogFactory.getLog(HEFT.class);
	Map<String, Map<Double, Double>> freeTimeSlotLengthsPerNode;
	Map<String, TreeSet<Double>> freeTimeSlotStartsPerNode;

	Map<TaskInstance, Double> readyTimePerTask;

	Map<String, Map<String, Double>> runtimeEstimate;

	public HEFT(Map<String, Map<String, Double>> runtimeEstimate) {
		super(runtimeEstimate);
		this.runtimeEstimate = runtimeEstimate;
		readyTimePerTask = new HashMap<>();
		freeTimeSlotStartsPerNode = new HashMap<>();
		freeTimeSlotLengthsPerNode = new HashMap<>();
		for (String node : runtimeEstimate.keySet()) {
			TreeSet<Double> occupiedTimeSlotStarts = new TreeSet<>();
			occupiedTimeSlotStarts.add(0d);
			freeTimeSlotStartsPerNode.put(node, occupiedTimeSlotStarts);
			Map<Double, Double> freeTimeSlotLengths = new HashMap<>();
			freeTimeSlotLengths.put(0d, Double.MAX_VALUE);
			freeTimeSlotLengthsPerNode.put(node, freeTimeSlotLengths);
		}
	}

	@Override
	public void addTask(TaskInstance task) {
		super.addTask(task);
		Collection<String> nodes = runtimeEstimate.keySet();
		double readyTime = readyTimePerTask.get(task);

		String bestNode = null;
		double bestNodeFreeTimeSlotActualStart = Double.MAX_VALUE;
		double bestFinish = Double.MAX_VALUE;

		for (String node : nodes) {
			double computationCost = runtimeEstimate.get(node).get(task.getTaskName());

			// the readytime of this task will have been set by now, as all predecessor tasks have a higher upward
			// rank and thus have been assigned to a vm already
			TreeSet<Double> freeTimeSlotStarts = freeTimeSlotStartsPerNode.get(node);
			Map<Double, Double> freeTimeSlotLengths = freeTimeSlotLengthsPerNode.get(node);

			SortedSet<Double> freeTimeSlotStartsAfterReadyTime = (freeTimeSlotStarts.floor(readyTime) != null) ? freeTimeSlotStarts
					.tailSet(freeTimeSlotStarts.floor(readyTime)) : freeTimeSlotStarts.tailSet(freeTimeSlotStarts
					.ceiling(readyTime));

			for (double freeTimeSlotStart : freeTimeSlotStartsAfterReadyTime) {
				double freeTimeSlotActualStart = Math.max(readyTime, freeTimeSlotStart);
				if (freeTimeSlotActualStart + computationCost > bestFinish)
					break;
				double freeTimeSlotLength = freeTimeSlotLengths.get(freeTimeSlotStart);
				if (freeTimeSlotActualStart > freeTimeSlotStart)
					freeTimeSlotLength -= freeTimeSlotActualStart - freeTimeSlotStart;
				if (computationCost < freeTimeSlotLength) {
					bestNode = node;
					bestNodeFreeTimeSlotActualStart = freeTimeSlotActualStart;
					bestFinish = freeTimeSlotActualStart + computationCost;
				}
			}
		}

		// assign task to node
		schedule.put(task, bestNode);
		log.info("Task " + task + " scheduled on node " + bestNode);
		if (task.readyToExecute()) {
			addTaskToQueue(task);
		}

		// update readytime of all successor tasks
		try {
			for (TaskInstance child : task.getChildTasks()) {
				if (bestFinish > readyTimePerTask.get(child)) {
					readyTimePerTask.put(child, bestFinish);
				}
			}
		} catch (WorkflowStructureUnknownException e) {
			throw new RuntimeException(e);
		}

		double timeslotStart = freeTimeSlotStartsPerNode.get(bestNode).floor(bestNodeFreeTimeSlotActualStart);
		double timeslotLength = freeTimeSlotLengthsPerNode.get(bestNode).get(timeslotStart);
		double diff = bestNodeFreeTimeSlotActualStart - timeslotStart;
		// add time slots before and after
		if (bestNodeFreeTimeSlotActualStart > timeslotStart) {
			freeTimeSlotLengthsPerNode.get(bestNode).put(timeslotStart, diff);
		} else {
			freeTimeSlotStartsPerNode.get(bestNode).remove(timeslotStart);
			freeTimeSlotLengthsPerNode.get(bestNode).remove(timeslotStart);
		}

		double computationCost = bestFinish - bestNodeFreeTimeSlotActualStart;
		double actualTimeSlotLength = timeslotLength - diff;
		if (computationCost < actualTimeSlotLength) {
			freeTimeSlotStartsPerNode.get(bestNode).add(bestFinish);
			freeTimeSlotLengthsPerNode.get(bestNode).put(bestFinish, actualTimeSlotLength - computationCost);
		}
	}

	@Override
	public void addTasks(Collection<TaskInstance> tasks) {
		List<TaskInstance> taskList = new LinkedList<>(tasks);
		Collections.sort(taskList, StaticTaskInstance.Comparators.DEPTH);

		Collection<String> nodes = runtimeEstimate.keySet();

		// compute upward ranks of all tasks
		for (int i = taskList.size() - 1; i >= 0; i--) {
			TaskInstance task = taskList.get(i);
			readyTimePerTask.put(task, 0d);
			double maxSuccessorRank = 0;
			try {
				for (TaskInstance child : task.getChildTasks()) {
					if (child.getUpwardRank() > maxSuccessorRank) {
						maxSuccessorRank = child.getUpwardRank();
					}
				}
			} catch (WorkflowStructureUnknownException e) {
				throw new RuntimeException(e);
			}

			double averageComputationCost = 0;
			for (String node : nodes) {
				averageComputationCost += runtimeEstimate.get(node).get(task.getTaskName());
			}
			averageComputationCost /= nodes.size();

			// note that the upward rank of a task will always be greater than that of its successors
			try {
				task.setUpwardRank(averageComputationCost + maxSuccessorRank);
			} catch (WorkflowStructureUnknownException e) {
				throw new RuntimeException(e);
			}
		}

		// Phase 1: Task Prioritizing (sort by decreasing order of rank)
		Collections.sort(taskList, StaticTaskInstance.Comparators.UPWARDSRANK);

		// Phase 2: Processor Selection
		for (TaskInstance task : taskList) {
			addTask(task);
		}

	}

}
