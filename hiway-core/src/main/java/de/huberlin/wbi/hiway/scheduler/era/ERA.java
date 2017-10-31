package de.huberlin.wbi.hiway.scheduler.era;

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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.scheduler.WorkflowScheduler;

public class ERA extends WorkflowScheduler {

	public static double alpha = 0.2;
	public static int rho = 1;

	protected Map<Long, Queue<TaskInstance>> readyTasksPerBot;
	protected Map<Long, Set<TaskInstance>> runningTasksPerBot;
	// protected Map<String, Map<Long, WienerProcessModel>> runtimePerBotPerVm;

	protected List<TaskInstance> replicas;
	protected Set<TaskInstance> todo;

	protected Map<TaskInstance, List<Container>> taskToContainers;

	protected boolean init;

	public ERA(String workflowName, HiWayConfiguration conf) {
		super(workflowName);
		readyTasksPerBot = new HashMap<>();
		runningTasksPerBot = new HashMap<>();
		replicas = new LinkedList<>();
		todo = new HashSet<>();
		taskToContainers = new HashMap<>();
		init = true;

		alpha = conf.getDouble(HiWayConfiguration.HIWAY_SCHEDULER_ERA_ALPHA, HiWayConfiguration.HIWAY_SCHEDULER_ERA_ALPHA_DEFAULT);
		rho = conf.getInt(HiWayConfiguration.HIWAY_SCHEDULER_ERA_RHO, HiWayConfiguration.HIWAY_SCHEDULER_ERA_RHO_DEFAULT);
	}

	@Override
	protected void addTask(TaskInstance task) {
		todo.add(task);
		long taskId = task.getTaskId();
		String taskName = task.getTaskName();

		WorkflowDriver.writeToStdout("Adding task of id " + taskId + " and name " + taskName + ".");
		numberOfRemainingTasks++;

		if (!getTaskIds().contains(taskId)) {
			newTask(taskId);
			WorkflowDriver.writeToStdout("Detected new BoT " + taskId + " (" + taskName + ").");
		}

		if (task.readyToExecute())
			addTaskToQueue(task);
	}

	@Override
	public void addTaskToQueue(TaskInstance task) {
		if (init) {
			for (int i = 0; i < rho; i++) {
				unissuedContainerRequests.add(setupContainerAskForRM(new String[0], containerMemory));
			}
			init = false;
		}
		unissuedContainerRequests.add(setupContainerAskForRM(new String[0], containerMemory));
		if (!readyTasksPerBot.containsKey(task.getTaskId())) {
			Queue<TaskInstance> q = new LinkedList<>();
			readyTasksPerBot.put(task.getTaskId(), q);
		}
		readyTasksPerBot.get(task.getTaskId()).add(task);
		WorkflowDriver.writeToStdout("Added task " + task + " to queue " + task.getTaskName());
	}

	@Override
	public TaskInstance getTask(Container container) {
		numberOfRemainingTasks--;
		numberOfRunningTasks++;

		String nodeId = container.getNodeId().getHost();
		if (!runtimePerBotPerVm.containsKey(nodeId)) {
			newHost(nodeId);
		}

		boolean replicate = false;

		Map<Long, Queue<TaskInstance>> b_ready = readyTasksPerBot;
		Map<Long, Set<TaskInstance>> b_run = runningTasksPerBot;

		Map<Long, Queue<TaskInstance>> b_select = b_ready;
		if (b_ready.isEmpty()) {
			b_select = new HashMap<>();
			for (Entry<Long, Set<TaskInstance>> e : b_run.entrySet()) {
				Queue<TaskInstance> tasks = new LinkedList<>(e.getValue());
				b_select.put(e.getKey(), tasks);
			}
			replicate = true;
		}

		Set<String> m = runtimePerBotPerVm.keySet();
		Queue<TaskInstance> b_min = null;
		double s_min = Double.MAX_VALUE;
		for (Entry<Long, Queue<TaskInstance>> b_i : b_select.entrySet()) {
			double e_j = runtimePerBotPerVm.get(nodeId).get(b_i.getKey()).getEstimate(System.currentTimeMillis(), replicate ? 0.5 : alpha);
			if (e_j == 0) {
				if (!replicate) {
					b_min = b_i.getValue();
					break;
				}
				e_j = Double.MAX_VALUE;
			}

			double e_min = Double.MAX_VALUE;
			double e_sum = e_j;
			int e_num = 1;
			for (String k : m) {
				double e_k = runtimePerBotPerVm.get(k).get(b_i.getKey()).getEstimate(System.currentTimeMillis(), replicate ? 0.5 : alpha);
				if (k.equals(nodeId) || e_k == 0)
					continue;
				if (e_k < e_min) {
					e_min = e_k;
				}
				e_sum += e_k;
				e_num++;
			}

			double s = (e_j - e_min) / (e_sum / e_num);
			if (s < s_min) {
				s_min = s;
				b_min = b_i.getValue();
			}
		}

		if (b_min != null && !b_min.isEmpty()) {
			TaskInstance task = b_min.remove();
			double estimate = runtimePerBotPerVm.get(nodeId).get(task.getTaskId()).getEstimate(System.currentTimeMillis(), replicate ? 0.5 : alpha);

			if (replicate) {
				replicas.add(task);
			} else {
				if (!runningTasksPerBot.containsKey(task.getTaskId())) {
					Set<TaskInstance> s = new HashSet<>();
					runningTasksPerBot.put(task.getTaskId(), s);
				}
				runningTasksPerBot.get(task.getTaskId()).add(task);

				if (readyTasksPerBot.get(task.getTaskId()).isEmpty()) {
					readyTasksPerBot.remove(task.getTaskId());
				}

			}

			if (!taskToContainers.containsKey(task)) {
				taskToContainers.put(task, new ArrayList<Container>());
			}
			taskToContainers.get(task).add(container);

			if (replicate) {
				WorkflowDriver
				    .writeToStdout("Assigned speculative copy of task " + task + " to container " + container.getId() + "@" + container.getNodeId().getHost());
			} else {
				WorkflowDriver.writeToStdout("Assigned task " + task + " to container " + container.getId() + "@" + container.getNodeId().getHost());
			}

			WorkflowDriver.writeToStdout("(estimate: " + estimate + "; sufferage: " + s_min + ")");
			task.incTries();

			return task;
		}

		return null;

	}

	@Override
	public int getNumberOfReadyTasks() {
		int nReadyTasks = 0;
		for (Queue<TaskInstance> queue : readyTasksPerBot.values())
			nReadyTasks += queue.size();
		return nReadyTasks;
	}

	@Override
	public int getNumberOfRunningTasks() {
		return taskToContainers.size();
	}

	@Override
	public int getNumberOfTotalTasks() {
		return getNumberOfFinishedTasks() + todo.size();
	}

	@Override
	public boolean nothingToSchedule() {
		return (todo.isEmpty() || (readyTasksPerBot.isEmpty() && !(replicas.size() < rho)));
	}

	@Override
	public Collection<ContainerId> taskCompleted(TaskInstance task, ContainerStatus containerStatus, long runtimeInMs) {
		super.taskCompleted(task, containerStatus, runtimeInMs);

		todo.remove(task);
		taskFinished(task);

		updateRuntimeEstimates(task.getWorkflowId().toString());

		// kill speculative copies
		Collection<ContainerId> toBeReleasedContainers = new ArrayList<>();
		for (Container container : taskToContainers.get(task)) {
			if (!container.getId().equals(containerStatus.getContainerId())) {
				toBeReleasedContainers.add(container.getId());
				unissuedContainerRequests.add(setupContainerAskForRM(new String[0], containerMemory));
			}
		}
		taskToContainers.remove(task);

		return toBeReleasedContainers;
	}

	@Override
	public Collection<ContainerId> taskFailed(TaskInstance task, ContainerStatus containerStatus) {
		super.taskFailed(task, containerStatus);

		taskFinished(task);

		Collection<ContainerId> toBeReleasedContainers = new ArrayList<>();
		if (!task.retry(maxRetries)) {
			for (Container container : taskToContainers.get(task)) {
				if (!container.getId().equals(containerStatus.getContainerId())) {
					toBeReleasedContainers.add(container.getId());
				}
			}
			taskToContainers.remove(task);
		}

		return toBeReleasedContainers;
	}

	private void taskFinished(TaskInstance task) {
		while (replicas.contains(task))
			replicas.remove(task);
		if (runningTasksPerBot.containsKey(task.getTaskId())) {
			runningTasksPerBot.get(task.getTaskId()).remove(task);
			if (runningTasksPerBot.get(task.getTaskId()).isEmpty())
				runningTasksPerBot.remove(task.getTaskId());
		}
	}

}
