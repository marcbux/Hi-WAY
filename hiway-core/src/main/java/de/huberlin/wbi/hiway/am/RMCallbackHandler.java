/**
 * <p>
 * The Heterogeneity-incorporating Workflow ApplicationMaster for YARN (Hi-WAY) provides the means to execute arbitrary scientific workflows on top of <a
 * href="http://hadoop.apache.org/">Apache's Hadoop 2.2.0 (YARN)</a>. In this context, scientific workflows are directed acyclic graphs (DAGs), in which nodes
 * are executables accessible from the command line (e.g. tar, cat, or any other executable in the PATH of the worker nodes), and edges represent data
 * dependencies between these executables.
 * </p>
 * 
 * <p>
 * Hi-WAY currently supports the workflow languages <a href="http://pegasus.isi.edu/wms/docs/latest/creating_workflows.php">Pegasus DAX</a> and <a
 * href="https://github.com/joergen7/cuneiform">Cuneiform</a> as well as the workflow schedulers static round robin, HEFT, greedy queue and ERA. Hi-WAY uses
 * Hadoop's distributed file system HDFS to store the workflow's input, output and intermediate data. The ApplicationMaster has been tested for up to 320
 * concurrent tasks and is fault-tolerant in that it is able to restart failed tasks.
 * </p>
 * 
 * <p>
 * When executing a scientific workflow, Hi-WAY requests a container from YARN's ResourceManager for each workflow task that is ready to execute. A task is
 * ready to execute once all its input data is available, i.e., all its data dependencies are resolved. The worker nodes on which containers are to be allocated
 * as well as the task assigned to an allocated container depend on the selected scheduling strategy.
 * </p>
 * 
 * <p>
 * The Hi-WAY ApplicationMaster is based on Hadoop's DistributedShell.
 * </p>
 */
package de.huberlin.wbi.hiway.am;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

	private WorkflowDriver am;
	// a data structure storing the invocation launched by each container
	private Map<ContainerId, HiWayInvocation> containerIdToInvocation = new HashMap<>();

	// a queue for allocated containers that have yet to be assigned a task
	private Queue<Container> containerQueue = new LinkedList<>();

	public RMCallbackHandler(WorkflowDriver am) {
		super();
		this.am = am;
	}

	@SuppressWarnings("unchecked")
	private ContainerRequest findFirstMatchingRequest(Container container) {
		List<? extends Collection<ContainerRequest>> requestCollections = am.getScheduler().relaxLocality() ? am.getAmRMClient().getMatchingRequests(
				container.getPriority(), ResourceRequest.ANY, container.getResource()) : am.getAmRMClient().getMatchingRequests(container.getPriority(),
				container.getNodeId().getHost(), container.getResource());

		for (Collection<ContainerRequest> requestCollection : requestCollections) {
			ContainerRequest request = requestCollection.iterator().next();
			if (request.getCapability().equals(container.getResource()))
				return request;
		}
		return null;
	}

	@Override
	public float getProgress() {
		// set progress to deliver to RM on next heartbeat
		if (am.getScheduler() == null)
			return 0f;
		int totalTasks = am.getScheduler().getNumberOfTotalTasks();
		float progress = (totalTasks == 0) ? 0 : (float) am.getNumCompletedContainers().get() / totalTasks;
		return progress;
	}

	protected void launchTask(TaskInstance task, Container allocatedContainer) {
		containerIdToInvocation.put(allocatedContainer.getId(), new HiWayInvocation(task));

		LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, am.getContainerListener(), task, am);
		Thread launchThread = new Thread(runnableLaunchContainer);

		/* launch and start the container on a separate thread to keep the main thread unblocked as all containers may not be allocated at one go. */
		am.getLaunchThreads().add(launchThread);
		launchThread.start();
	}

	protected void launchTasks() {
		while (!containerQueue.isEmpty() && !am.getScheduler().nothingToSchedule()) {
			Container allocatedContainer = containerQueue.remove();

			long tic = System.currentTimeMillis();
			TaskInstance task = am.getScheduler().getTask(allocatedContainer);
			long toc = System.currentTimeMillis();

			if (task.getTries() == 1) {
				JSONObject obj = new JSONObject();
				try {
					obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
				} catch (JSONException e) {
					onError(e);
				}
				task.getReport().add(
						new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), Long.valueOf(task.getId()),
								null, HiwayDBI.KEY_INVOC_TIME_SCHED, obj));
				task.getReport().add(
						new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), Long.valueOf(task.getId()),
								null, HiwayDBI.KEY_INVOC_HOST, allocatedContainer.getNodeId().getHost()));
			}
			launchTask(task, allocatedContainer);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		if (HiWayConfiguration.verbose) {
			for (Container container : allocatedContainers) {
				WorkflowDriver.writeToStdout("Allocated container " + container.getId().getContainerId() + " on node " + container.getNodeId().getHost()
						+ " with capability " + container.getResource().getVirtualCores() + ":" + container.getResource().getMemory());
			}
		}

		for (Container container : allocatedContainers) {
			JSONObject value = new JSONObject();
			try {
				value.put("type", "container-allocated");
				value.put("container-id", container.getId());
				value.put("node-id", container.getNodeId());
				value.put("node-http", container.getNodeHttpAddress());
				value.put("memory", container.getResource().getMemory());
				value.put("vcores", container.getResource().getVirtualCores());
				value.put("service", container.getContainerToken().getService());
			} catch (JSONException e) {
				onError(e);
			}

			am.writeEntryToLog(new JsonReportEntry(am.getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
			ContainerRequest request = findFirstMatchingRequest(container);

			if (request != null) {
				if (HiWayConfiguration.verbose)
					WorkflowDriver.writeToStdout("Removing container request " + request.getNodes() + ":" + request.getCapability().getVirtualCores() + ":"
							+ request.getCapability().getMemory());
				am.getAmRMClient().removeContainerRequest(request);
				am.getNumAllocatedContainers().incrementAndGet();
				containerQueue.add(container);
			} else {
				if (HiWayConfiguration.verbose)
					WorkflowDriver.writeToStdout("Releasing container " + container.getId().getContainerId() + " on node " + container.getNodeId().getHost()
							+ " with capability " + container.getResource().getVirtualCores() + ":" + container.getResource().getMemory());
				am.getAmRMClient().releaseAssignedContainer(container.getId());
			}
		}

		launchTasks();
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> completedContainers) {
		for (ContainerStatus containerStatus : completedContainers) {

			JSONObject value = new JSONObject();
			try {
				value.put("type", "container-completed");
				value.put("container-id", containerStatus.getContainerId());
				value.put("state", containerStatus.getState());
				value.put("exit-code", containerStatus.getExitStatus());
				value.put("diagnostics", containerStatus.getDiagnostics());
			} catch (JSONException e) {
				onError(e);
			}
			am.writeEntryToLog(new JsonReportEntry(am.getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));

			// non complete containers should not be here
			assert (containerStatus.getState() == ContainerState.COMPLETE);

			// increment counters for completed/failed containers
			int exitStatus = containerStatus.getExitStatus();
			String diagnostics = containerStatus.getDiagnostics();
			ContainerId containerId = containerStatus.getContainerId();

			// The container was released by the framework (e.g., it was a speculative copy of a finished task)
			if (diagnostics.equals(SchedulerUtils.RELEASED_CONTAINER)) {
				WorkflowDriver.writeToStdout("Container was released." + ", containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
						+ ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			} else if (exitStatus == ExitCode.FORCE_KILLED.getExitCode()) {
				WorkflowDriver.writeToStdout("Container was force killed." + ", containerID=" + containerStatus.getContainerId() + ", state="
						+ containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			} else if (containerIdToInvocation.containsKey(containerId)) {

				HiWayInvocation invocation = containerIdToInvocation.get(containerStatus.getContainerId());
				TaskInstance finishedTask = invocation.task;

				if (exitStatus == 0) {
					// this task might have been completed previously (e.g., via speculative replication)
					if (!finishedTask.isCompleted()) {
						finishedTask.setCompleted();

						am.evaluateReport(finishedTask, containerId);

						for (JsonReportEntry entry : finishedTask.getReport()) {
							am.writeEntryToLog(entry);
						}

						long runtime = System.currentTimeMillis() - invocation.timestamp;
						JSONObject obj = new JSONObject();
						try {
							obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(runtime));
						} catch (JSONException e) {
							e.printStackTrace(System.out);
							System.exit(-1);
						}
						am.writeEntryToLog(new JsonReportEntry(System.currentTimeMillis(), finishedTask.getWorkflowId(), finishedTask.getTaskId(), finishedTask
								.getTaskName(), finishedTask.getLanguageLabel(), finishedTask.getId(), null, JsonReportEntry.KEY_INVOC_TIME, obj));

						Collection<ContainerId> toBeReleasedContainers = am.getScheduler().taskCompleted(finishedTask, containerStatus, runtime);
						for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
							WorkflowDriver.writeToStdout("Killing speculative copy of task " + finishedTask + " on container " + toBeReleasedContainer);
							am.getAmRMClient().releaseAssignedContainer(toBeReleasedContainer);
							am.getNumKilledContainers().incrementAndGet();
						}

						am.getNumCompletedContainers().incrementAndGet();

						am.taskSuccess(finishedTask, containerId);
					}
				}

				// The container failed horribly.
				else {

					am.taskFailure(finishedTask, containerId);
					am.getNumFailedContainers().incrementAndGet();

					if (exitStatus == ExitCode.TERMINATED.getExitCode()) {
						WorkflowDriver.writeToStdout("Container was terminated." + ", containerID=" + containerStatus.getContainerId() + ", state="
								+ containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
								+ containerStatus.getDiagnostics());
					} else {
						WorkflowDriver.writeToStdout("Container completed with failure." + ", containerID=" + containerStatus.getContainerId() + ", state="
								+ containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
								+ containerStatus.getDiagnostics());

						Collection<ContainerId> toBeReleasedContainers = am.getScheduler().taskFailed(finishedTask, containerStatus);
						for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
							WorkflowDriver.writeToStdout("Killing speculative copy of task " + finishedTask + " on container " + toBeReleasedContainer);
							am.getAmRMClient().releaseAssignedContainer(toBeReleasedContainer);
							am.getNumKilledContainers().incrementAndGet();
						}
					}
				}
			}

			/* The container was aborted by the framework without it having been assigned an invocation (e.g., because the RM allocated more containers than
			 * requested) */
			else {
				WorkflowDriver.writeToStdout("Container failed." + ", containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
						+ ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			}
		}

		launchTasks();
	}

	@Override
	public void onError(Throwable e) {
		e.printStackTrace(System.out);
		System.exit(-1);
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
	}

	@Override
	public void onShutdownRequest() {
		WorkflowDriver.writeToStdout("Shutdown Request.");
		am.setDone();
	}
}
