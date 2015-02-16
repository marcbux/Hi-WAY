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
package de.huberlin.wbi.hiway.scheduler.c3po;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import de.huberlin.hiwaydb.useDB.InvocStat;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.scheduler.Estimate;
import de.huberlin.wbi.hiway.scheduler.RuntimeEstimate;
import de.huberlin.wbi.hiway.scheduler.Scheduler;

/**
 * A basic implementation of the <b>C</b>loning, <b>C</b>urious, <b>C</b>onservative, <b>P</b>lacement-aware, <b>O</b>utlooking (C3PO) workflow scheduler.
 * Consider the following example as a showcase for how C3PO operates.
 * 
 * <p>
 * Imagine there's three kinds of task instances in a given workflow: Shake, Rattle, and Roll. Or &ndash; to put it in MapReduce terminology &ndash; there's
 * three jobs, Shake, Rattle, and Roll, each of which comprises a multitude of tasks. Say that we're currently in the middle of executing this workflow on three
 * machines: Charlie, Tango, and Foxtrot. Currently, Tango and Foxtrot are working on some previously assigned task and Charlie is waiting to be assigned a task
 * by the C3PO scheduler. How does C3PO decide, which kind of task to assign to Charlie?
 * </p>
 * 
 * <p>
 * If no tasks are available for assignment, C3PO assigns a speculative copy of a task that's already running on a different machine (Tango or Foxtrot) to
 * Charlie. If any of a task's clones finish execution (including the original task), the execution of all other clones is cancelled. The selection policy for
 * speculative copies of tasks is similar to that of regular tasks, as outlined in detail below. The main intend of C3PO's <b>Cloning</b> strategy is to speedup
 * execution at computationally intensive bottlenecks within a workflow. The mantra of the <b>Cloning</b> strategy is <i>"Nobody Waits"</i>.
 * </p>
 * 
 * <p>
 * Assume however for the rest of this example that there are tasks of all three jobs (Shake, Rattle, and Roll) available. Then, C3PO checks whether Charlie has
 * executed at least one of each kind of tasks (Shake, Rattle, and Roll). Say if Charlie hadn't executed a Rattle task yet, it would now be assigned one. This
 * behavior is called the <b>Curiosity</b> principle: C3PO is curious how well Charlie executes Rattle tasks, hence it simply tries it out. The rationale behind
 * the <b>Curiosity</b> principle is that in order to make good decisions about which task to assign to which worker, C3PO needs to know how adapt each worker
 * is at executing each task. If there is no data available, C3PO cannot make an educated decision.
 * </p>
 * 
 * <p>
 * Let us assume that Charlie has executed at least one task of each job before though. Hence, C3PO has gathered statistics on previous task executions. These
 * could look as follows:
 * </p>
 * 
 * <p>
 * <table border="1">
 * <tr>
 * <th>runtime estimates</th>
 * <th><b>Shake</b></th>
 * <th><b>Rattle</b></th>
 * <th><b>Roll</b></th>
 * </tr>
 * <tr>
 * <td>Charlie</td>
 * <td>5 (0.167)</td>
 * <td>10 (0.5)</td>
 * <td>20 (0.2)</td>
 * </tr>
 * <tr>
 * <td>Tango</td>
 * <td>10 (0.333)</td>
 * <td>5 (0.25)</td>
 * <td>50 (0.5)</td>
 * </tr>
 * <tr>
 * <td>Foxtrot</td>
 * <td>15 (0.5)</td>
 * <td>5 (0.25)</td>
 * <td>30 (0.3)</td>
 * </tr>
 * </table>
 * <i>These runtime estimates are based on past runtime measurements. How exactly the estimates are derived from measurements depends on the concrete
 * implementation of C3PO. In the default approach, the runtime of the last task execution serves as the runtime estimate for the next task execution. The
 * values in brackets correspond to the normalized, task-specific runtime estimates across all machines. These values serve as an indicator for how well a task
 * is suited to a machine, i.e., how well the machine fares at executing this task in comparison to all other machines. For instance, Charlie appears to be
 * comparably good at running Shake and Roll tasks.</i>
 * </p>
 * 
 * <p>
 * <table border="1">
 * <tr>
 * <th>contribution to workflow runtime</th>
 * <th><b>Shake</b></th>
 * <th><b>Rattle</b></th>
 * <th><b>Roll</b></th>
 * </tr>
 * <tr>
 * <td>average task runtime</td>
 * <td>8</td>
 * <td>7</td>
 * <td>30</td>
 * </tr>
 * <tr>
 * <td>remaining tasks</td>
 * <td>50</td>
 * <td>100</td>
 * <td>30</td>
 * </tr>
 * <tr>
 * <td>combined runtime estimate</td>
 * <td>400 (0.2)</td>
 * <td>700 (0.35)</td>
 * <td>900 (0.45)</td>
 * </tr>
 * </table>
 * <i>The total runtime that each job (collection of similar tasks) contributes to the remaining workflow execution time. The first row contains the average
 * runtime of a task across all machines. The number of remaining tasks of a certain type can be found in the second row. The third row lists the product, which
 * can be used as an estimate of the combined runtime of all remaining tasks of similar kind. The numbers in brackets correspond to the normalized values, i.e.,
 * the relative share with which each job contributes to overall estimated runtime.</i>
 * </p>
 * 
 * <p>
 * These measurements provide C3PO with two important scheduling guidelines:
 * <ol>
 * <li><b>Conservatism</b>, measured as the suitability of each task for being executed on each machine and inferred from its runtime estimates. According the
 * the mantra "<i>Do what you do best</i>", C3PO attempts to assign tasks to machines which have proven to execute these tasks with above-average runtime.</li>
 * <li><b>Outlook</b>: If tasks are assigned to machines based purely on the notion of suitability, tasks which contribute strongest to overall workload will be
 * left over at the end. By favoring the assignments of these kinds of tasks from the beginning C3PO is able to harness the benefits of worker specialization
 * until the end of workflow execution. This principle is also called "<i>Business before Pleasure</i>".</li>
 * </ol>
 * Based on these two guidelines, C3PO selects an appropriate task via sampling. For instance, Charlie would most likely &ndash; though not necessarily &ndash;
 * be assigned a Roll task: Judging from its past runtime estimates it is highly suited to execute Roll tasks (<b>Conservatism</b>:
 * "<i>Do what you do best</i>") and Roll tasks contribute stronger to overall remaining workload than the even better-suited Shake tasks (<b>Outlook</b>:
 * "<i>Don't be greedy</i>").
 * </p>
 * 
 * <p>
 * The sampling part of the algorithm is important as it prevents C3PO of getting stuck in local optima or never reconsidering task-machine assignments due to
 * distorted runtime measurements. Adapt users can choose how strong the Conservatism and Outlook values affect the sampling. If high values are chosen, C3PO
 * will always chose the best (i.e., <b>Conservatism</b>- and/or <b>Outlook</b>-optimizing) solution. For lower values, the behavior of C3PO will be
 * increasingly random.
 * </p>
 * 
 * <p>
 * Once C3PO has chosen the right kind of task for a machine (e.g., a Roll task for Charlie), it will attempt to find a concrete task, whose input data already
 * resides on the machine. This <b>Placement</b> awareness ensures that unnecessary data transfer is minimized.
 * </p>
 */
public class C3PO extends Scheduler {

	private static void normalizeWeights(Collection<? extends Estimate> statistics) {
		double sum = 0d;
		for (Estimate statistic : statistics)
			sum += statistic.weight;
		for (Estimate statistic : statistics)
			statistic.weight /= (sum != 0d) ? sum : statistics.size();
	}

	private double conservatismWeight = 1d;
	protected Map<Long, PlacementAwarenessEstimate> dataLocalityStatistics;
	private final DecimalFormat df;
	protected Map<Long, OutlookEstimate> jobStatistics;
	private int nClones = 0;
	private final Random numGen;
	private double outlookWeight = 1d;
	private double placementAwarenessWeight = 1d;
	// One queue of ready-to-execute tasks for each job, identified by its unique job name.
	protected Map<Long, Queue<TaskInstance>> readyTasks;
	protected Map<Long, Queue<TaskInstance>> runningTasks;
	protected Map<Long, String> taskIdToName;
	protected Map<TaskInstance, List<Container>> taskToContainers;

	public C3PO(String workflowName, FileSystem hdfs, HiWayConfiguration conf) {
		this(workflowName, hdfs, System.currentTimeMillis(), conf);
	}

	public C3PO(String workflowName, FileSystem hdfs, long seed, HiWayConfiguration conf) {
		super(workflowName, conf, hdfs);
		readyTasks = new HashMap<>();
		runningTasks = new HashMap<>();
		taskIdToName = new HashMap<>();
		taskToContainers = new HashMap<>();
		jobStatistics = new HashMap<>();
		dataLocalityStatistics = new HashMap<>();
		numGen = new Random(seed);
		Locale loc = new Locale("en");
		df = (DecimalFormat) NumberFormat.getNumberInstance(loc);
		df.applyPattern("###.##");
		df.setMaximumIntegerDigits(7);
	}

	public C3PO(String workflowName, HiWayConfiguration conf) {
		this(workflowName, System.currentTimeMillis(), conf);
	}

	public C3PO(String workflowName, long seed, HiWayConfiguration conf) {
		this(workflowName, null, seed, conf);
		this.placementAwarenessWeight = 0d;
	}

	@Override
	protected void addTask(TaskInstance task) {

		System.out.println("Adding task of id " + task.getTaskId() + " and name " + task.getTaskName());

		numberOfRemainingTasks++;
		long taskId = task.getTaskId();
		if (!getTaskIds().contains(taskId)) {
			newTask(taskId);
			taskIdToName.put(taskId, task.getTaskName());
			System.out.println("TaskId " + taskId + " (" + task.getTaskName() + ") to map.");
		}

		jobStatistics.get(taskId).remainingTasks++;
		if (task.readyToExecute())
			addTaskToQueue(task);
	}

	@Override
	public void addTaskToQueue(TaskInstance task) {
		unissuedNodeRequests.add(new String[0]);
		readyTasks.get(task.getTaskId()).add(task);
		System.out.println("Added task " + task + " to queue " + task.getTaskName());
	}

	/* Outlook: Zero probabiliy for tasks which are not currently ready (or - in the case of speculative execution - running) Equally high probability for tasks
	 * which have not been executed by any node; if no such tasks exist, assign higher probabilites to tasks which contribute stronger to overall runtime */
	private void computeJobStatisticsWeight(boolean replicate) {
		for (long taskId : getTaskIds()) {
			OutlookEstimate jobStatistic = jobStatistics.get(taskId);
			double avgRuntime = (jobStatistic.finishedTasks != 0) ? jobStatistic.timeSpent / jobStatistic.finishedTasks : 0d;
			if ((replicate && runningTasks.get(taskId).size() == 0) || (!replicate && readyTasks.get(taskId).size() == 0)) {
				jobStatistic.weight = 0;
			} else if (avgRuntime == 0d) {
				jobStatistic.weight = Long.MAX_VALUE;
			} else {
				jobStatistic.weight = jobStatistic.remainingTasks;
				if (replicate)
					jobStatistic.weight += runningTasks.get(taskId).size();
				jobStatistic.weight *= avgRuntime;
			}
		}
		normalizeWeights(jobStatistics.values());
		printJobStatisticsWeight();
	}

	private void computePlacementAwarenessWeights(Container container, boolean replicate) {
		for (long taskId : getTaskIds()) {
			Queue<TaskInstance> queue = replicate ? runningTasks.get(taskId) : readyTasks.get(taskId);
			PlacementAwarenessEstimate dataLocalityStatistic = dataLocalityStatistics.get(taskId);
			if (queue.size() == 0) {
				dataLocalityStatistic.weight = 0d;
			} else {
				TaskInstance task = queue.peek();
				try {
					/* in case of total data being zero (prevent division by zero if a container has no input data for ready tasks whatsoever) */
					dataLocalityStatistic.localData = task.countAvailableLocalData(container) + 1;
					/* in case of total data being zero (prevent division by zero) */
					dataLocalityStatistic.totalData = task.countAvailableTotalData() + 1;
					dataLocalityStatistic.weight = ((double) (dataLocalityStatistic.localData)) / ((double) dataLocalityStatistic.totalData);
				} catch (IOException e) {
					System.err.println("Error during hdfs block location determination.");
					e.printStackTrace();
					System.exit(-1);
				}
			}
		}
		normalizeWeights(dataLocalityStatistics.values());
		printPlacementAwarenessWeights(replicate);
	}

	/* Conservatism: Equally high probability for tasks which this node has not executed yet; if no such tasks exist, assign higher probabilities to tasks which
	 * this node is good at */
	private void computeTaskStatisticsWeights() {
		for (long taskId : getTaskIds()) {
			Collection<RuntimeEstimate> taskStatistics = new ArrayList<>();
			for (String nodeId : getNodeIds()) {
				RuntimeEstimate taskStatistic = runtimeEstimatesPerNode.get(nodeId).get(taskId);
				taskStatistic.weight = (taskStatistic.finishedTasks != 0) ? 1d / taskStatistic.averageRuntime : Long.MAX_VALUE;
				taskStatistics.add(taskStatistic);
			}
			normalizeWeights(taskStatistics);
		}
		printTaskStatisticsWeights();
		for (String nodeId : getNodeIds())
			normalizeWeights(runtimeEstimatesPerNode.get(nodeId).values());
	}

	@Override
	public TaskInstance getNextTask(Container container) {
		TaskInstance task = null;

		numberOfRemainingTasks--;
		numberOfRunningTasks++;

		boolean replicate = getNumberOfReadyTasks() == 0;

		String nodeId = container.getNodeId().getHost();
		if (!runtimeEstimatesPerNode.containsKey(nodeId)) {
			newHost(nodeId);
		}

		computeJobStatisticsWeight(replicate);
		computeTaskStatisticsWeights();
		computePlacementAwarenessWeights(container, replicate);

		Map<Long, Estimate> combinedWeights = new HashMap<>();
		for (long taskId : getTaskIds())
			combinedWeights.put(taskId, new Estimate());
		multiplyWeights(combinedWeights, runtimeEstimatesPerNode.get(nodeId), conservatismWeight);
		multiplyWeights(combinedWeights, jobStatistics, outlookWeight);
		multiplyWeights(combinedWeights, dataLocalityStatistics, placementAwarenessWeight);
		normalizeWeights(combinedWeights.values());

		System.out.println("Updated Decision Vector for node " + nodeId + ":");
		System.out.println("\tConservatism (x" + (int) (conservatismWeight + 0.5d) + ")\t" + printWeights(runtimeEstimatesPerNode.get(nodeId)));
		System.out.println("\tOutlook (x" + (int) (outlookWeight + 0.5d) + ")\t\t" + printWeights(jobStatistics));
		System.out.println("\tPlacement (x" + (int) (placementAwarenessWeight + 0.5d) + ")\t\t" + printWeights(dataLocalityStatistics));
		System.out.println("\tCombined\t\t" + printWeights(combinedWeights));

		double sample = numGen.nextDouble();
		double min = 0d;
		for (long taskId : getTaskIds()) {
			double max = min + combinedWeights.get(taskId).weight;
			if (sample < max) {
				Queue<TaskInstance> queue = runningTasks.get(taskId);
				if (!replicate) {
					jobStatistics.get(taskId).remainingTasks--;
					queue = readyTasks.get(taskId);
				}

				task = queue.remove();
				runningTasks.get(taskId).add(task);
				if (!taskToContainers.containsKey(task)) {
					taskToContainers.put(task, new ArrayList<Container>());
				}
				taskToContainers.get(task).add(container);

				if (replicate) {
					System.out.println("Assigned speculative copy of task " + task + " to container " + container.getId().getContainerId() + " on node "
							+ container.getNodeId().getHost());
				} else {
					System.out.println("Assigned task " + task + " to container " + container.getId().getContainerId() + " on node "
							+ container.getNodeId().getHost());
				}

				task.incTries();
				return task;
			}
			min = max;
		}

		return task;
	}

	@Override
	public int getNumberOfReadyTasks() {
		int nReadyTasks = 0;
		for (Queue<TaskInstance> queue : readyTasks.values())
			nReadyTasks += queue.size();
		return nReadyTasks;
	}

	@Override
	public int getNumberOfRunningTasks() {
		return taskToContainers.size();
	}

	@Override
	public int getNumberOfTotalTasks() {

		int fin = getNumberOfFinishedTasks();
		int run = getNumberOfRunningTasks();
		int rem = 0;
		for (OutlookEstimate jobStatistic : jobStatistics.values()) {
			rem += jobStatistic.remainingTasks;
		}
		System.out.println("Scheduled Containers Finished: " + fin);
		System.out.println("Scheduled Containers Running: " + run);
		System.out.println("Scheduled Containers Remaining: " + rem);

		return fin + run + rem;
	}

	public void init() {

	}

	@Override
	public void initialize() {
		super.initialize();
		System.out.println("HiwayDB: Querying Task Ids for workflow " + workflowName + " from database.");
		Collection<Long> newTaskIds = dbInterface.getTaskIdsForWorkflow(workflowName);
		System.out.println("HiwayDB: Retrieved Task Ids " + newTaskIds.toString() + " from database.");
		for (long taskId : newTaskIds) {
			System.out.println("HiwayDB: Querying Task Name for Task Id " + taskId + " from database.");
			String taskName = dbInterface.getTaskName(taskId);
			taskIdToName.put(taskId, taskName);
			System.out.println("HiwayDB: Retrieved Task Name " + taskName + " from database.");
		}
	}

	private void multiplyWeights(Map<Long, Estimate> weights, Map<Long, ? extends Estimate> statistics, double factor) {
		for (Long taskId : getTaskIds())
			weights.get(taskId).weight *= Math.pow(statistics.get(taskId).weight, factor);
	}

	@Override
	protected void newTask(long taskId) {
		super.newTask(taskId);
		for (Map<Long, RuntimeEstimate> runtimeEstimates : runtimeEstimatesPerNode.values()) {
			runtimeEstimates.put(taskId, new RuntimeEstimate());
		}
		jobStatistics.put(taskId, new OutlookEstimate());
		dataLocalityStatistics.put(taskId, new PlacementAwarenessEstimate());
		readyTasks.put(taskId, new LinkedList<TaskInstance>());
		runningTasks.put(taskId, new LinkedList<TaskInstance>());
	}

	@Override
	public boolean nothingToSchedule() {
		if (nClones > 0 && getNumberOfRunningTasks() > 0) {
			return false;
		}
		return getNumberOfReadyTasks() == 0;
	}

	private void printJobStatisticsWeight() {
		System.out.println("Updated Job Statistics:");

		System.out.println("\t\t#finish\tavg\t#remain\t#ready\tshare");
		for (long taskId : getTaskIds()) {
			String jobName = taskIdToName.get(taskId);
			String jobName7 = (jobName.length() > 7) ? jobName.substring(0, 7) : jobName;
			OutlookEstimate jobStatistic = jobStatistics.get(taskId);
			double avgRuntime = (jobStatistic.finishedTasks != 0) ? jobStatistic.timeSpent / jobStatistic.finishedTasks : 0d;
			System.out.println("\t" + jobName7 + "\t" + df.format(jobStatistic.finishedTasks) + "\t" + df.format(avgRuntime) + "\t"
					+ df.format(jobStatistic.remainingTasks) + "\t" + df.format(readyTasks.get(taskId).size()) + "\t" + df.format(jobStatistic.weight));
		}
	}

	private void printPlacementAwarenessWeights(boolean replicate) {
		System.out.println("Updated Placement Awareness Statistics:");

		System.out.println("\t\tlocal\ttotal\tshare");

		for (long taskId : getTaskIds()) {
			Queue<TaskInstance> queue = replicate ? runningTasks.get(taskId) : readyTasks.get(taskId);
			if (queue.size() != 0) {
				String jobName = taskIdToName.get(taskId);
				String jobName7 = (jobName.length() > 7) ? jobName.substring(0, 7) : jobName;
				PlacementAwarenessEstimate dataLocalityStatistic = dataLocalityStatistics.get(taskId);
				System.out.println("\t" + jobName7 + "\t" + dataLocalityStatistic.localData + "\t" + dataLocalityStatistic.totalData + "\t"
						+ df.format(dataLocalityStatistic.weight));
			}
		}
	}

	private void printTaskStatisticsWeights() {
		System.out.println("Updated Task Statistics:");

		String row = "";
		for (long taskId : getTaskIds()) {
			String jobName = taskIdToName.get(taskId);
			String jobName7 = (jobName.length() > 7) ? jobName.substring(0, 7) : jobName;
			row += "\t\t" + jobName7;
		}
		System.out.println(row);

		for (String nodeId : getNodeIds()) {
			String nodeName7 = (nodeId.length() > 7) ? nodeId.substring(nodeId.length() - 7) : nodeId;

			row = "";
			for (long taskId : getTaskIds()) {
				RuntimeEstimate taskStatistic = runtimeEstimatesPerNode.get(nodeId).get(taskId);
				row += "\t" + df.format(taskStatistic.averageRuntime) + "\t" + df.format(taskStatistic.weight);
			}

			System.out.println("\t" + nodeName7 + row);
		}
	}

	private String printWeights(Map<Long, ? extends Estimate> statistics) {
		String names = "";
		String weights = "";
		for (Long taskId : getTaskIds()) {
			names += ", " + taskIdToName.get(taskId);
			weights += ", " + df.format(statistics.get(taskId).weight);
		}
		return "(" + names.substring(2) + ")" + "\t" + "(" + weights.substring(2) + ")";
	}

	public void setConservatismWeight(double conservatismWeight) {
		this.conservatismWeight = conservatismWeight < Double.MIN_VALUE ? Double.MIN_VALUE : conservatismWeight;
	}

	public void setnClones(int nClones) {
		if (this.nClones < nClones) {
			for (int i = 0; i < nClones - this.nClones; i++) {
				unissuedNodeRequests.add(new String[0]);
			}
		} else {
			for (int i = 0; i < this.nClones - nClones; i++) {
				unissuedNodeRequests.remove();
			}
		}

		this.nClones = nClones > 0 ? nClones : 0;
	}

	public void setOutlookWeight(double outlookWeight) {
		this.outlookWeight = outlookWeight < Double.MIN_VALUE ? Double.MIN_VALUE : outlookWeight;
	}

	public void setPlacementAwarenessWeight(double placementAwarenessWeight) {
		this.placementAwarenessWeight = placementAwarenessWeight;
	}

	@Override
	public Collection<ContainerId> taskCompleted(TaskInstance task, ContainerStatus containerStatus, long runtimeInMs) {
		super.taskCompleted(task, containerStatus, runtimeInMs);
		updateRuntimeEstimates(task.getWorkflowId().toString());
		Collection<ContainerId> toBeReleasedContainers = new ArrayList<>();

		// kill speculative copies
		for (Container container : taskToContainers.get(task)) {
			if (!container.getId().equals(containerStatus.getContainerId())) {
				toBeReleasedContainers.add(container.getId());
				unissuedNodeRequests.add(new String[0]);
			}
		}
		taskToContainers.remove(task);
		runningTasks.get(task.getTaskId()).remove(task);

		return toBeReleasedContainers;
	}

	@Override
	public Collection<ContainerId> taskFailed(TaskInstance task, ContainerStatus containerStatus) {
		super.taskFailed(task, containerStatus);

		Collection<ContainerId> toBeReleasedContainers = new ArrayList<>();
		if (!task.retry(maxRetries)) {
			for (Container container : taskToContainers.get(task)) {
				if (!container.getId().equals(containerStatus.getContainerId())) {
					toBeReleasedContainers.add(container.getId());
				}
			}
			taskToContainers.remove(task);

			return toBeReleasedContainers;
		}

		return new ArrayList<>();
	}

	@Override
	protected void updateRuntimeEstimate(InvocStat stat) {
		super.updateRuntimeEstimate(stat);
		OutlookEstimate jobStatistic = jobStatistics.get(stat.getTaskId());
		jobStatistic.finishedTasks++;
		jobStatistic.timeSpent += stat.getRealTime();
	}

}
