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
package de.huberlin.wbi.hiway.scheduler.heft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;

import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;
import de.huberlin.wbi.hiway.scheduler.DepthComparator;
import de.huberlin.wbi.hiway.scheduler.StaticScheduler;

/**
 * <p>
 * The HEFT scheduler, as described in [1]. This implementation of HEFT does not yet make use of estimates for data transfer times between nodes.
 * </p>
 * 
 * <p>
 * [1] Topcuoglu, H., Hariri, S., and Wu, M.-Y. (2002). <i>Performance-Effective and Low-Complexity Task Scheduling for Heterogeneous Computing.</i> IEEE
 * Transactions on Parallel and Distributed Systems, 13(3), 260-274.
 * </p>
 * 
 * @author Marc Bux
 * 
 */
public class HEFT extends StaticScheduler {

	private int containers;

	private Map<String, ArrayList<Map<Double, Double>>> freeTimeSlotLengthsPerNode;
	private Map<String, ArrayList<TreeSet<Double>>> freeTimeSlotStartsPerNode;
	private Map<TaskInstance, Double> readyTimePerTask;

	private Map<String, ArrayList<TreeMap<Double, TaskInstance>>> taskOnsetsPerNode;

	public HEFT(String workflowName, FileSystem hdfs, HiWayConfiguration conf, int containers) {
		super(workflowName, hdfs, conf);

		this.containers = containers;
		System.out.println("HEFT has detected " + containers + " containers per worker node.");

		readyTimePerTask = new HashMap<>();
		freeTimeSlotStartsPerNode = new HashMap<>();
		freeTimeSlotLengthsPerNode = new HashMap<>();
		taskOnsetsPerNode = new HashMap<>();
	}

	@Override
	protected void addTask(TaskInstance task) {
		numberOfRemainingTasks++;
		Collection<String> nodes = runtimeEstimatesPerNode.keySet();

		// the readytime of a task is the time from workflow onset after which this task will be ready (according to the schedule)
		// if the task is an input task, its ready time will still be 0
		// otherwise, its readytime will have been set already as all predecessor tasks have a higher upward rank will have been scheduled already (during which
		// this task's readytime was updated)
		double readyTime = readyTimePerTask.get(task);

		String bestNode = null;
		int bestI = -1;
		double bestNodeFreeTimeSlotActualStart = Double.MAX_VALUE;
		double bestFinish = Double.MAX_VALUE;

		// compute the finish time of executing this task on each node; chose the earliest finish time
		for (String node : nodes) {
			ArrayList<TreeSet<Double>> freeTimeSlotStartsPerContainer = freeTimeSlotStartsPerNode.get(node);
			ArrayList<Map<Double, Double>> freeTimeSlotLengthsPerContainer = freeTimeSlotLengthsPerNode.get(node);

			// note that the weight is 1 ms if no runtime estimate is available yet
			double computationCost = runtimeEstimatesPerNode.get(node).get(task.getTaskId()).weight;

			for (int i = 0; i < containers; i++) {
				// note that at the beginning, all nodes have a free timeslot of length MAX_VALUE starting at time 0
				TreeSet<Double> freeTimeSlotStarts = freeTimeSlotStartsPerContainer.get(i);
				Map<Double, Double> freeTimeSlotLengths = freeTimeSlotLengthsPerContainer.get(i);

				// freeTimeSlotStartsAfterReadyTime is the (sorted) set of timeslots available once this task is ready
				SortedSet<Double> freeTimeSlotStartsAfterReadyTime = (freeTimeSlotStarts.floor(readyTime) != null) ? freeTimeSlotStarts
						.tailSet(freeTimeSlotStarts.floor(readyTime)) : freeTimeSlotStarts.tailSet(freeTimeSlotStarts.ceiling(readyTime));

				// iterate through timeslots
				for (double freeTimeSlotStart : freeTimeSlotStartsAfterReadyTime) {

					// task onset could be later than timeslot onset
					double freeTimeSlotActualStart = Math.max(readyTime, freeTimeSlotStart);

					// if finishtime of task on this node is worse than the current optimum, we don't have to look at this node any more
					if (freeTimeSlotActualStart + computationCost > bestFinish)
						break;

					// otherwise, we found a new best node, but only if the computation cost does not exceed the length of the timeslot
					double freeTimeSlotLength = freeTimeSlotLengths.get(freeTimeSlotStart);
					if (freeTimeSlotActualStart > freeTimeSlotStart)
						freeTimeSlotLength -= freeTimeSlotActualStart - freeTimeSlotStart;
					if (computationCost < freeTimeSlotLength) {
						bestNode = node;
						bestI = i;
						bestNodeFreeTimeSlotActualStart = freeTimeSlotActualStart;
						bestFinish = freeTimeSlotActualStart + computationCost;
					}
				}
			}
		}

		// assign task to node
		schedule.put(task, bestNode);
		taskOnsetsPerNode.get(bestNode).get(bestI).put(bestNodeFreeTimeSlotActualStart, task);
		if (HiWayConfiguration.verbose)
			System.out.println("Task " + task + " scheduled on node " + bestNode);
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
			e.printStackTrace();
			System.exit(-1);
		}

		// once the task has been scheduled on a node, update that node's free timeslots
		double timeslotStart = freeTimeSlotStartsPerNode.get(bestNode).get(bestI).floor(bestNodeFreeTimeSlotActualStart);
		double timeslotLength = freeTimeSlotLengthsPerNode.get(bestNode).get(bestI).get(timeslotStart);
		double diff = bestNodeFreeTimeSlotActualStart - timeslotStart;

		// add time slot before
		if (bestNodeFreeTimeSlotActualStart > timeslotStart) {
			freeTimeSlotLengthsPerNode.get(bestNode).get(bestI).put(timeslotStart, diff);
		} else {
			freeTimeSlotStartsPerNode.get(bestNode).get(bestI).remove(timeslotStart);
			freeTimeSlotLengthsPerNode.get(bestNode).get(bestI).remove(timeslotStart);
		}

		// add time slot after
		double computationCost = bestFinish - bestNodeFreeTimeSlotActualStart;
		double actualTimeSlotLength = timeslotLength - diff;
		if (computationCost < actualTimeSlotLength) {
			freeTimeSlotStartsPerNode.get(bestNode).get(bestI).add(bestFinish);
			freeTimeSlotLengthsPerNode.get(bestNode).get(bestI).put(bestFinish, actualTimeSlotLength - computationCost);
		}
	}

	@Override
	public void addTasks(Collection<TaskInstance> tasks) {
		if (queues.size() == 0) {
			System.out.println("No provenance data available for static scheduling. Aborting.");
			System.exit(-1);
		}

		// obtain task list and sort by depth
		List<TaskInstance> taskList = new LinkedList<>(tasks);
		Collections.sort(taskList, new DepthComparator());

		Collection<String> nodes = runtimeEstimatesPerNode.keySet();

		// compute upward ranks of all tasks
		for (int i = taskList.size() - 1; i >= 0; i--) {
			TaskInstance task = taskList.get(i);
			readyTimePerTask.put(task, 0d);

			// maxSuccessorRank is the maximum upward rank of the current task's children
			double maxSuccessorRank = 0;
			try {
				for (TaskInstance child : task.getChildTasks()) {
					if (child.getUpwardRank() > maxSuccessorRank) {
						maxSuccessorRank = child.getUpwardRank();
					}
				}
			} catch (WorkflowStructureUnknownException e) {
				e.printStackTrace();
				System.exit(-1);
			}

			// averageComputationCost is the average cost for executing this task on any node
			double averageComputationCost = 0;
			for (String node : nodes) {
				averageComputationCost += runtimeEstimatesPerNode.get(node).get(task.getTaskId()).weight;
			}
			averageComputationCost /= nodes.size();

			// set upward rank of this task
			// note that the upward rank of a task will always be greater than that of its successors
			try {
				task.setUpwardRank(averageComputationCost + maxSuccessorRank);
			} catch (WorkflowStructureUnknownException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		// Phase 1: Task Prioritizing (sort by decreasing order of rank)
		Collections.sort(taskList, new UpwardsRankComparator());

		// Phase 2: Processor Selection; schedule tasks w/ higher rank (critical tasks at the beginning of the wf) first
		for (TaskInstance task : taskList) {
			addTask(task);
		}

		printSchedule();
	}

	@Override
	protected void newHost(String nodeId) {
		super.newHost(nodeId);

		ArrayList<TreeSet<Double>> freeTimeSlotStartsPerContainer = new ArrayList<>();
		freeTimeSlotStartsPerNode.put(nodeId, freeTimeSlotStartsPerContainer);
		for (int i = 0; i < containers; i++) {
			TreeSet<Double> freeTimeSlotStarts = new TreeSet<>();
			freeTimeSlotStarts.add(0d);
			freeTimeSlotStartsPerContainer.add(freeTimeSlotStarts);
		}

		ArrayList<Map<Double, Double>> freeTimeSlotLengthsPerContainer = new ArrayList<>();
		freeTimeSlotLengthsPerNode.put(nodeId, freeTimeSlotLengthsPerContainer);
		for (int i = 0; i < containers; i++) {
			Map<Double, Double> freeTimeSlotLengths = new HashMap<>();
			freeTimeSlotLengths.put(0d, Double.MAX_VALUE);
			freeTimeSlotLengthsPerContainer.add(freeTimeSlotLengths);
		}

		ArrayList<TreeMap<Double, TaskInstance>> taskOnsetsPerContainer = new ArrayList<>();
		taskOnsetsPerNode.put(nodeId, taskOnsetsPerContainer);
		for (int i = 0; i < containers; i++) {
			TreeMap<Double, TaskInstance> taskOnsets = new TreeMap<>();
			taskOnsetsPerContainer.add(taskOnsets);
		}
	}

	public void printSchedule() {
		StringBuilder sb = new StringBuilder("HEFT Schedule:\n");
		for (String node : taskOnsetsPerNode.keySet()) {
			ArrayList<TreeMap<Double, TaskInstance>> taskOnsetsPerContainer = taskOnsetsPerNode.get(node);
			for (int i = 0; i < containers; i++) {
				TreeMap<Double, TaskInstance> taskOnsets = taskOnsetsPerContainer.get(i);
				sb.append(node);
				sb.append("-");
				sb.append(i + 1);
				sb.append(": ");
				for (Double onset : taskOnsets.keySet()) {
					sb.append(onset);
					sb.append("-");
					TaskInstance task = taskOnsets.get(onset);
					sb.append(task.getTaskName());
					sb.append("-");
					sb.append(onset + runtimeEstimatesPerNode.get(node).get(task.getTaskId()).weight);
					sb.append(" ");
				}
				sb.append("\n");
			}
		}
		System.out.print(sb.toString());
	}

}
