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
package de.huberlin.wbi.hiway.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.scheduler.Scheduler;
import de.huberlin.wbi.hiway.scheduler.C3PO;
import de.huberlin.wbi.hiway.scheduler.GreedyQueue;
import de.huberlin.wbi.hiway.scheduler.HEFT;
import de.huberlin.wbi.hiway.scheduler.StaticRoundRobin;

/**
 * <p>
 * The Heterogeneity-incorporating Workflow ApplicationMaster for YARN (Hi-WAY)
 * provides the means to execute arbitrary scientific workflows on top of <a
 * href="http://hadoop.apache.org/">Apache's Hadoop 2.2.0 (YARN)</a>. In this
 * context, scientific workflows are directed acyclic graphs (DAGs), in which
 * nodes are executables accessible from the command line (e.g. tar, cat, or any
 * other executable in the PATH of the worker nodes), and edges represent data
 * dependencies between these executables.
 * </p>
 * 
 * <p>
 * Hi-WAY currently supports the workflow languages <a
 * href="http://pegasus.isi.edu/wms/docs/latest/creating_workflows.php">Pegasus
 * DAX</a> and <a href="https://github.com/joergen7/cuneiform">Cuneiform</a> as
 * well as the workflow schedulers static round robin, HEFT, greedy queue and
 * C3PO. Hi-WAY uses Hadoop's distributed file system HDFS to store the
 * workflow's input, output and intermediate data. The ApplicationMaster has
 * been tested for up to 320 concurrent tasks and is fault-tolerant in that it
 * is able to restart failed tasks.
 * </p>
 * 
 * <p>
 * When executing a scientific workflow, Hi-WAY requests a container from YARN's
 * ResourceManager for each workflow task that is ready to execute. A task is
 * ready to execute once all its input data is available, i.e., all its data
 * dependencies are resolved. The worker nodes on which containers are to be
 * allocated as well as the task assigned to an allocated container depend on
 * the selected scheduling strategy.
 * </p>
 * 
 * <p>
 * The Hi-WAY ApplicationMaster is based on Hadoop's DistributedShell.
 * </p>
 */
public abstract class ApplicationMaster {

	// an internal class that stores a task along with some additional
	// information
	private class HiWayInvocation {
		final TaskInstance task;
		final long timestamp;

		public HiWayInvocation(TaskInstance task) {
			this.task = task;
			timestamp = System.currentTimeMillis();
		}
	}

	public abstract UUID getRunId();

	public abstract void parseWorkflow();

	public abstract void taskFailure(TaskInstance task, ContainerId containerId);

	public abstract void taskSuccess(TaskInstance task, ContainerId containerId);

	/**
	 * Thread to connect to the {@link ContainerManagementProtocol} and launch
	 * the container that will execute the shell command.
	 */
	private class LaunchContainerRunnable implements Runnable {
		Container container;
		NMCallbackHandler containerListener;
		TaskInstance task;

		/**
		 * @param lcontainer
		 *            Allocated container
		 * @param containerListener
		 *            Callback handler of the container
		 */
		public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener, TaskInstance task) {
			this.container = lcontainer;
			this.containerListener = containerListener;
			this.task = task;
		}

		/**
		 * Connects to CM, sets up container launch context for shell command
		 * and eventually dispatches the container start request to the CM.
		 */
		@Override
		public void run() {
			log.info("Setting up container launch container for containerid=" + container.getId());
			ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

			// Set the environment
			StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*");
			for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
				classPathEnv.append(':');
				classPathEnv.append(File.pathSeparatorChar);
				classPathEnv.append(c.trim());
			}

			if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
				classPathEnv.append(':');
				classPathEnv.append(System.getProperty("java.class.path"));
			}

			shellEnv.put("CLASSPATH", classPathEnv.toString());

			// Set the environment
			ctx.setEnvironment(shellEnv);

			// Set the local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			try {
				String containerId = container.getId().toString();
				File script = new File(containerId + ".sh");
				try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(script))) {
					scriptWriter.write(task.getCommand());
				} catch (IOException e) {
					e.printStackTrace();
				}
				Data scriptData = new Data(script.getPath());
				try {
					scriptData.stageOut(fs, containerId);
				} catch (IOException e) {
					e.printStackTrace();
				}
				scriptData.addToLocalResourceMap(localResources, fs, containerId);
			} catch (IOException e1) {
				log.info("Error during Container startup. exiting");
				shutdown(e1);
			}
			ctx.setLocalResources(localResources);

			// Set the necessary command to execute on the allocated container
			Vector<CharSequence> vargs = new Vector<CharSequence>(5);

			vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
			// Set Xmx based on am memory size
			vargs.add("-Xmx" + containerMemory + "m");
			// Set class name
			vargs.add(HiWayConfiguration.HIWAY_WORKER_CLASS);

			vargs.add("--appId " + appId.toString());
			vargs.add("--containerId " + container.getId().toString());
			vargs.add("--workflowId " + task.getWorkflowId());
			vargs.add("--taskId " + task.getTaskId());
			vargs.add("--taskName " + task.getTaskName());
			vargs.add("--langLabel " + task.getLanguageLabel());
			vargs.add("--signature " + task.getSignature());
			for (Data inputData : task.getInputData()) {
				vargs.add("--input " + inputData.getLocalPath() + "," + inputData.isInput() + "," + Data.hdfsDirectoryMidfixes.get(inputData));
			}
			for (Data outputData : task.getOutputData()) {
				vargs.add("--output " + outputData.getLocalPath());
			}
			if (determineFileSizes) {
				vargs.add("--size");
			}

			// Get final commmand
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}

			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());
			ctx.setCommands(commands);

			// Set up tokens for the container. For normal shell commands,
			// the container in distribute-shell doesn't need any tokens. We are
			// populating them mainly for NodeManagers to be able to download
			// any
			// files in the distributed file-system. The tokens are otherwise
			// also
			// useful in cases, for e.g., when one is running a "hadoop dfs"
			// command
			// inside the distributed shell.
			ctx.setTokens(allTokens.duplicate());

			containerListener.addContainer(container.getId(), container);
			nmClientAsync.startContainerAsync(container, ctx);
		}
	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		private final ApplicationMaster applicationMaster;
		private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

		public NMCallbackHandler(ApplicationMaster applicationMaster) {
			this.applicationMaster = applicationMaster;
		}

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
			if (log.isDebugEnabled()) {
				log.debug("Succeeded to start Container " + containerId);
			}
			Container container = containers.get(containerId);
			if (container != null) {
				nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
			}
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			if (log.isDebugEnabled()) {
				log.debug("Container Status: id=" + containerId + ", status=" + containerStatus);
			}
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			if (log.isDebugEnabled()) {
				log.debug("Succeeded to stop Container " + containerId);
			}
			containers.remove(containerId);
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
			log.error("Failed to query the status of Container " + containerId);
			shutdown(t);
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			log.error("Failed to start Container " + containerId);
			shutdown(t);
			containers.remove(containerId);
			applicationMaster.numCompletedContainers.incrementAndGet();
			applicationMaster.numFailedContainers.incrementAndGet();
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			log.error("Failed to stop Container " + containerId);
			shutdown(t);
			containers.remove(containerId);
		}
	}

	protected class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
		@SuppressWarnings("unchecked")
		private ContainerRequest findFirstMatchingRequest(Container container) {
			List<? extends Collection<ContainerRequest>> requestCollections = scheduler.relaxLocality() ? amRMClient.getMatchingRequests(
					container.getPriority(), ResourceRequest.ANY, container.getResource()) : amRMClient.getMatchingRequests(container.getPriority(), container
					.getNodeId().getHost(), container.getResource());

			for (Collection<ContainerRequest> requestCollection : requestCollections) {
				for (ContainerRequest request : requestCollection) {
					return request;
				}
			}
			return null;
		}

		@Override
		public float getProgress() {
			// set progress to deliver to RM on next heartbeat
			if (scheduler == null)
				return 0f;
			int totalTasks = scheduler.getNumberOfTotalTasks();
			float progress = (totalTasks == 0) ? 0 : (float) numCompletedContainers.get() / totalTasks;
			return progress;
		}

		protected void launchTask(TaskInstance task, Container allocatedContainer) {
			containerIdToInvocation.put(allocatedContainer.getId().getId(), new HiWayInvocation(task));
			log.info("Launching workflow task on a new container." + ", task=" + task + ", containerId=" + allocatedContainer.getId() + ", containerNode="
					+ allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
					+ allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());

			// try {
			// buildScripts(task, allocatedContainer);

			LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener, task);
			Thread launchThread = new Thread(runnableLaunchContainer);

			// launch and start the container on a separate thread to
			// keep the main thread unblocked as all
			// containers may not be allocated at one go.
			launchThreads.add(launchThread);
			launchThread.start();
			metrics.endWaitingTask();
			metrics.runningTask(task);
			metrics.launchedTask(task);

		}

		protected void launchTasks() {
			while (!containerQueue.isEmpty() && !scheduler.nothingToSchedule()) {
				Container allocatedContainer = containerQueue.remove();

				long tic = System.currentTimeMillis();
				TaskInstance task = scheduler.getNextTask(allocatedContainer);
				long toc = System.currentTimeMillis();

				if (task.getTries() == 1) {
					JSONObject obj = new JSONObject();
					try {
						obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
					} catch (JSONException e) {
						e.printStackTrace();
					}
					task.getReport().add(
							new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getSignature(), null,
									HiwayDBI.KEY_INVOC_TIME_SCHED, obj));
					task.getReport().add(
							new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getSignature(), null,
									HiwayDBI.KEY_INVOC_HOST, allocatedContainer.getNodeId().getHost()));
				}
				launchTask(task, allocatedContainer);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
			log.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());

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
					e.printStackTrace();
				}

				writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
				ContainerRequest request = findFirstMatchingRequest(container);

				if (request != null) {
					amRMClient.removeContainerRequest(request);
					numAllocatedContainers.incrementAndGet();
					containerQueue.add(container);
				} else {
					amRMClient.releaseAssignedContainer(container.getId());
				}
			}

			launchTasks();
		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			log.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
			for (ContainerStatus containerStatus : completedContainers) {

				JSONObject value = new JSONObject();
				try {
					value.put("type", "container-completed");
					value.put("container-id", containerStatus.getContainerId());
					value.put("state", containerStatus.getState());
					value.put("exit-code", containerStatus.getExitStatus());
					value.put("diagnostics", containerStatus.getDiagnostics());
				} catch (JSONException e) {
					e.printStackTrace();
				}

				log.info("Got container status for containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus="
						+ containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
				writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));

				// non complete containers should not be here
				assert (containerStatus.getState() == ContainerState.COMPLETE);

				// increment counters for completed/failed containers
				int exitStatus = containerStatus.getExitStatus();
				String diagnostics = containerStatus.getDiagnostics();
				ContainerId containerId = containerStatus.getContainerId();

				if (containerIdToInvocation.containsKey(containerId.getId())) {

					HiWayInvocation invocation = containerIdToInvocation.get(containerStatus.getContainerId().getId());
					TaskInstance finishedTask = invocation.task;

					if (exitStatus == 0) {

						log.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());

						// this task might have been completed previously (e.g.,
						// via speculative replication)
						if (!finishedTask.isCompleted()) {
							finishedTask.setCompleted();

							evaluateReport(finishedTask, containerId);

							for (JsonReportEntry entry : finishedTask.getReport()) {
								writeEntryToLog(entry);
							}

							Collection<ContainerId> toBeReleasedContainers = scheduler.taskCompleted(finishedTask, containerStatus, System.currentTimeMillis()
									- invocation.timestamp);
							for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
								log.info("Killing speculative copy of task " + finishedTask + " on container " + toBeReleasedContainer);
								amRMClient.releaseAssignedContainer(toBeReleasedContainer);
								numKilledContainers.incrementAndGet();
							}

							numCompletedContainers.incrementAndGet();
							metrics.completedTask(finishedTask);
							metrics.endRunningTask(finishedTask);

							taskSuccess(finishedTask, containerId);
						}
					}

					// The container was released by the framework (e.g., it was
					// a speculative copy of a finished task)
					else if (diagnostics.equals(SchedulerUtils.RELEASED_CONTAINER)) {
						log.info("Container was released." + ", containerId=" + containerStatus.getContainerId());
					}

					else if (exitStatus == ExitCode.FORCE_KILLED.getExitCode()) {
						log.info("Container was force killed." + ", containerId=" + containerStatus.getContainerId());
					}

					// The container failed horribly.
					else {

						taskFailure(finishedTask, containerId);
						numFailedContainers.incrementAndGet();
						metrics.failedTask(finishedTask);

						if (exitStatus == ExitCode.TERMINATED.getExitCode()) {
							log.info("Container was terminated." + ", containerId=" + containerStatus.getContainerId());
						} else {
							log.info("Container completed with failure." + ", containerId=" + containerStatus.getContainerId());

							Collection<ContainerId> toBeReleasedContainers = scheduler.taskFailed(finishedTask, containerStatus);
							for (ContainerId toBeReleasedContainer : toBeReleasedContainers) {
								log.info("Killing speculative copy of task " + finishedTask + " on container " + toBeReleasedContainer);
								amRMClient.releaseAssignedContainer(toBeReleasedContainer);
								numKilledContainers.incrementAndGet();

							}
						}
					}
				}

				// The container was aborted by the framework without it having
				// been assigned an invocation
				// (e.g., because the RM allocated more containers than
				// requested)
				else {

				}
			}

			launchTasks();
		}

		@Override
		public void onError(Throwable e) {
			shutdown(e);
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
		}

		@Override
		public void onShutdownRequest() {
			log.info("Shutdown Request.");
			done = true;
		}
	}

	// public static int hdfsInstancesPerContainer;

	// a handle to the log, in which any events are recorded
	private static final Log log = LogFactory.getLog(ApplicationMaster.class);

	protected static final Log statLog = LogFactory.getLog("statLogger");

	/**
	 * The main routine.
	 * 
	 * @param args
	 *            Command line arguments passed to the ApplicationMaster.
	 */
	public static void loop(ApplicationMaster appMaster, String[] args) {
		boolean result = false;
		try {
			log.info("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			result = appMaster.run();
		} catch (Throwable t) {
			log.fatal("Error running ApplicationMaster", t);
			appMaster.shutdown(t);
		}
		if (result) {
			log.info("Application Master completed successfully. exiting");
			System.exit(0);
		} else {
			log.info("Application Master failed. exiting");
			System.exit(2);
		}
	}

	protected AMRMClientAsync.CallbackHandler allocListener;
	// the yarn tokens to be passed to any launched containers
	protected ByteBuffer allTokens;
	// a handle to the YARN ResourceManager
	@SuppressWarnings("rawtypes")
	protected AMRMClientAsync amRMClient;
	// this application's attempt id (combination of attemptId and fail count)
	protected ApplicationAttemptId appAttemptID;
	// the internal id assigned to this application by the YARN ResourceManager
	protected String appId;
	// the hostname of the container running the Hi-WAY ApplicationMaster
	protected String appMasterHostname = "";
	// the port on which the ApplicationMaster listens for status updates from
	// clients
	protected int appMasterRpcPort = -1;
	// the tracking URL to which the ApplicationMaster publishes info for
	// clients to monitor
	protected String appMasterTrackingUrl = "";
	// the configuration of the Hadoop installation
	protected Configuration conf;
	protected HiWayConfiguration hiWayConf;
	protected int containerCores = 1;
	// a data structure storing the invocation launched by each container
	protected Map<Integer, HiWayInvocation> containerIdToInvocation = new HashMap<>();
	// a listener for processing the responses from the NodeManagers
	protected NMCallbackHandler containerListener;
	// the memory and number of virtual cores to request for the container on
	// which the workflow tasks are launched
	protected int containerMemory = 4096;
	// a queue for allocated containers that have yet to be assigned a task
	protected Queue<Container> containerQueue = new LinkedList<>();
	// flags denoting workflow execution has finished and been successful
	protected volatile boolean done;
	// the report, in which provenance information is stored
	private Data federatedReport;
	// private BufferedWriter federatedReportWriter;
	protected Map<String, Data> files = new HashMap<>();
	// a handle to the hdfs
	protected FileSystem fs;
	// a list of threads, one for each container launch
	protected List<Thread> launchThreads = new ArrayList<Thread>();
	// a structure that stores various metrics during workflow execution
	protected final WFAppMetrics metrics = WFAppMetrics.create();
	// a handle to communicate with the YARN NodeManagers
	protected NMClientAsync nmClientAsync;
	// a counter for allocated containers
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	// a counter for completed containers (complete denotes successful or failed
	protected AtomicInteger numCompletedContainers = new AtomicInteger();
	// a counter for failed containers
	protected AtomicInteger numFailedContainers = new AtomicInteger();
	// a counter for killed containers
	protected AtomicInteger numKilledContainers = new AtomicInteger();
	// a counter for requested containers
	protected AtomicInteger numRequestedContainers = new AtomicInteger();
	// priority of the container request
	protected int requestPriority;
	// the workflow scheduler, as defined at workflow launch time
	protected Scheduler scheduler;
	protected HiWayConfiguration.HIWAY_SCHEDULER_OPTS schedulerName;
	// environment variables to be passed to any launched containers
	protected Map<String, String> shellEnv = new HashMap<String, String>();
	protected volatile boolean success;
	protected Data workflowFile;
	// the workflow to be executed along with its format and path in the file
	// system
	protected String workflowPath;
	protected boolean determineFileSizes = false;
	private String sandboxDir;
	private String summaryFile;
	protected Map<Long, JsonReportEntry> taskIdContainerRequestedEvent = new HashMap<>();
	protected Map<Long, JsonReportEntry> taskIdContainerAllocatedEvent = new HashMap<>();

	public ApplicationMaster() {
		conf = new YarnConfiguration();
		conf.addResource("core-site.xml");
		hiWayConf = new HiWayConfiguration();
		// conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") +
		// "/core-site.xml"));
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * If the debug flag is set, dump out contents of current working directory
	 * and the environment to stdout for debugging.
	 */
	private void dumpOutDebugInfo() {
		log.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			log.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
		}

		String cmd = "ls -al";
		Runtime run = Runtime.getRuntime();
		Process pr = null;
		try {
			pr = run.exec(cmd);
			pr.waitFor();

			BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line = "";
			while ((line = buf.readLine()) != null) {
				log.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
			}
			buf.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void finish() {
		writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_WF_TIME, Long.toString(System.currentTimeMillis()
				- amRMClient.getStartTime())));

		// Join all launched threads needed for when we time out and we need to
		// release containers
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				log.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}

		// When the application completes, it should stop all running containers
		log.info("Application completed. Stopping running containers");
		nmClientAsync.stop();

		// When the application completes, it should send a finish application
		// signal to the RM
		log.info("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		success = true;

		log.info("Failed Containers: " + numFailedContainers.get());
		log.info("Completed Containers: " + numCompletedContainers.get());

		int numTotalContainers = scheduler.getNumberOfTotalTasks();

		log.info("Total Scheduled Containers: " + numTotalContainers);

		if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
			// metrics.completedWorkflow(workflow);
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			// metrics.failedWorkflow(workflow);
			appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + numCompletedContainers.get() + ", allocated="
					+ numAllocatedContainers.get() + ", failed=" + numFailedContainers.get() + ", killed=" + numKilledContainers.get();
			success = false;
		}

		try {
			federatedReport.stageOut(fs, "");
			if (summaryFile != null) {
				String stdout = fs.getHomeDirectory().toString() + "/" + sandboxDir + "/" + appId + "/AppMaster.stdout";
				String stderr = fs.getHomeDirectory().toString() + "/" + sandboxDir + "/" + appId + "/AppMaster.stderr";
				String statlog = fs.getHomeDirectory().toString() + "/" + sandboxDir + "/" + appId + "/"
						+ hiWayConf.get(HiWayConfiguration.HIWAY_DB_STAT_LOG, HiWayConfiguration.HIWAY_DB_STAT_LOG_DEFAULT);

				try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFile))) {
					Collection<String> output = new ArrayList<>();
					for (Data outputFile : getOutputFiles()) {
						output.add(fs.getHomeDirectory().toString() + "/" + outputFile.getHdfsPath(Data.hdfsDirectoryMidfixes.get(outputFile)));
					}
					JSONObject obj = new JSONObject();
					try {
						obj.put("output", output);
						obj.put("stdout", stdout);
						obj.put("stderr", stderr);
						obj.put("statlog", statlog);
					} catch (JSONException e) {
						e.printStackTrace();
					}
					writer.write(obj.toString());
				}
				new Data("AppMaster.stdout").stageOut(fs, "");
				new Data("AppMaster.stderr").stageOut(fs, "");
				new Data(summaryFile).stageOut(fs, "");
			}
		} catch (IOException e) {
			log.info("Error when attempting to stage out federated output log.");
			shutdown(e);
		}

		try {
			amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (YarnException | IOException e) {
			log.error("Failed to unregister application", e);
			shutdown(e);
		}

		amRMClient.stop();
	}

	public Collection<Data> getOutputFiles() {
		Collection<Data> outputFiles = new ArrayList<>();

		for (Data data : files.values()) {
			if (data.isOutput()) {
				outputFiles.add(data);
			}
		}

		return outputFiles;
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	public String getWorkflowName() {
		return workflowFile.getName();
	}

	/**
	 * Parse command line options.
	 * 
	 * @param args
	 *            Command line arguments.
	 * @return Whether init successful and run should be invoked.
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean init(String[] args) throws ParseException {

		DefaultMetricsSystem.initialize("ApplicationMaster");

		Options opts = new Options();
		opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
		opts.addOption("workflow", true, "The workflow file to be executed by the Application Master");
		opts.addOption("s", "summary", true, "The name of the json summary file. No file is created if this parameter is not specified.");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("appid", true, "Id of this Application Master.");

		opts.addOption("help", false, "Print usage");
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException("No args specified for application master to initialize");
		}

		if (!cliParser.hasOption("appid")) {
			throw new IllegalArgumentException("No id of Application Master specified");
		}

		appId = cliParser.getOptionValue("appid");

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}

		if (cliParser.hasOption("summary")) {
			summaryFile = cliParser.getOptionValue("summary");
		}

		sandboxDir = hiWayConf.get(HiWayConfiguration.HIWAY_AM_SANDBOX_DIRECTORY, HiWayConfiguration.HIWAY_AM_SANDBOX_DIRECTORY_DEFAULT);
		Data.setHdfsDirectoryPrefix(sandboxDir + "/" + appId);

		Map<String, String> envs = System.getenv();

		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException("Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
		}

		log.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
				+ appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId=" + appAttemptID.getAttemptId());

		String shellEnvs[] = hiWayConf.getStrings(HiWayConfiguration.HIWAY_WORKER_SHELL_ENV, HiWayConfiguration.HIWAY_WORKER_SHELL_ENV_DEFAULT);
		for (String env : shellEnvs) {
			env = env.trim();
			int index = env.indexOf('=');
			if (index == -1) {
				shellEnv.put(env, "");
				continue;
			}
			String key = env.substring(0, index);
			String val = "";
			if (index < (env.length() - 1)) {
				val = env.substring(index + 1);
			}
			shellEnv.put(key, val);
		}
		// }

		if (!cliParser.hasOption("workflow")) {
			throw new IllegalArgumentException("No workflow file specified to be executed by application master");
		}

		workflowPath = cliParser.getOptionValue("workflow");
		workflowFile = new Data(workflowPath);
		schedulerName = HiWayConfiguration.HIWAY_SCHEDULER_OPTS.valueOf(hiWayConf.get(HiWayConfiguration.HIWAY_SCHEDULER,
				HiWayConfiguration.HIWAY_SCHEDULER_DEFAULT.toString()));

		containerMemory = hiWayConf.getInt(HiWayConfiguration.HIWAY_WORKER_MEMORY, HiWayConfiguration.HIWAY_WORKER_MEMORY_DEFAULT);
		containerCores = hiWayConf.getInt(HiWayConfiguration.HIWAY_WORKER_VCORES, HiWayConfiguration.HIWAY_WORKER_VCORES_DEFAULT);
		requestPriority = hiWayConf.getInt(HiWayConfiguration.HIWAY_WORKER_PRIORITY, HiWayConfiguration.HIWAY_WORKER_PRIORITY_DEFAULT);
		return true;
	}

	/**
	 * Helper function to print usage.
	 * 
	 * @param opts
	 *            Parsed command line options.
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	/**
	 * Main run function for the application master
	 * 
	 * @throws YarnException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public boolean run() throws YarnException, IOException {
		log.info("Starting ApplicationMaster");

		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(conf);
		amRMClient.start();

		containerListener = new NMCallbackHandler(this);
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		Data workflowFile = new Data(workflowPath);
		workflowFile.stageIn(fs, "");

		// Register self with ResourceManager. This will start heartbeating to
		// the RM.
		appMasterHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

		switch (schedulerName) {
		case staticRoundRobin:
		case heft:
			scheduler = schedulerName.equals(HiWayConfiguration.HIWAY_SCHEDULER_OPTS.staticRoundRobin) ? new StaticRoundRobin(getWorkflowName(), fs, hiWayConf)
					: new HEFT(getWorkflowName(), fs, hiWayConf);
			break;
		case greedyQueue:
			scheduler = new GreedyQueue(getWorkflowName(), hiWayConf, fs);
			break;
		default:
			C3PO c3po = new C3PO(getWorkflowName(), fs, hiWayConf);
			switch (schedulerName) {
			case conservative:
				c3po.setConservatismWeight(12d);
				c3po.setnClones(0);
				c3po.setPlacementAwarenessWeight(0.01d);
				c3po.setOutlookWeight(0.01d);
				break;
			case cloning:
				c3po.setConservatismWeight(0.01d);
				c3po.setnClones(1);
				c3po.setPlacementAwarenessWeight(0.01d);
				c3po.setOutlookWeight(0.01d);
				break;
			case placementAware:
				c3po.setConservatismWeight(0.01d);
				c3po.setnClones(0);
				c3po.setPlacementAwarenessWeight(12d);
				c3po.setOutlookWeight(0.01d);
				break;
			case outlooking:
				c3po.setConservatismWeight(0.01d);
				c3po.setnClones(0);
				c3po.setPlacementAwarenessWeight(0.01d);
				c3po.setOutlookWeight(12d);
				break;
			default:
				c3po.setConservatismWeight(3d);
				c3po.setnClones(2);
				c3po.setPlacementAwarenessWeight(1d);
				c3po.setOutlookWeight(2d);
			}
			scheduler = c3po;
		}

		scheduler.initialize();
		writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_WF_NAME, getWorkflowName()));
		parseWorkflow();
		scheduler.updateRuntimeEstimates(getRunId().toString());
		federatedReport = new Data(hiWayConf.get(HiWayConfiguration.HIWAY_DB_STAT_LOG, HiWayConfiguration.HIWAY_DB_STAT_LOG_DEFAULT));

		// Dump out information about cluster capability as seen by the resource
		// manager
		int maxMem = response.getMaximumResourceCapability().getMemory();
		int maxCores = response.getMaximumResourceCapability().getVirtualCores();
		log.info("Max mem capabililty of resources in this cluster " + maxMem);

		// A resource ask cannot exceed the max.
		if (containerMemory > maxMem) {
			log.info("Container memory specified above max threshold of cluster." + " Using max value." + ", specified=" + containerMemory + ", max=" + maxMem);
			containerMemory = maxMem;
		}
		if (containerCores > maxCores) {
			log.info("Container vcores specified above max threshold of cluster." + " Using max value." + ", specified=" + containerCores + ", max=" + maxCores);
			containerCores = maxCores;
		}

		while (!done) {
			try {
				while (scheduler.hasNextNodeRequest()) {
					ContainerRequest containerAsk = setupContainerAskForRM(scheduler.getNextNodeRequest());
					amRMClient.addContainerRequest(containerAsk);
				}
				Thread.sleep(1000);
				log.info("Current application state: requested=" + numRequestedContainers + ", completed=" + numCompletedContainers + ", failed="
						+ numFailedContainers + ", killed=" + numKilledContainers + ", allocated=" + numAllocatedContainers);
			} catch (InterruptedException ex) {
			}
		}
		finish();

		return success;
	}

	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 * 
	 * @param nodes
	 *            The worker nodes on which this container is to be allocated.
	 *            If left empty, the container will be launched on any worker
	 *            node fulfilling the resource requirements.
	 * @return the setup ResourceRequest to be sent to RM
	 */
	private ContainerRequest setupContainerAskForRM(String[] nodes) {
		metrics.waitingTask();

		// set the priority for the request
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(requestPriority);

		// set up resource type requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(containerMemory);
		capability.setVirtualCores(containerCores);

		ContainerRequest request = new ContainerRequest(capability, nodes, null, pri, scheduler.relaxLocality());
		JSONObject value = new JSONObject();
		try {
			value.put("type", "container-requested");
			value.put("memory", capability.getMemory());
			value.put("vcores", capability.getVirtualCores());
			value.put("nodes", nodes);
			value.put("priority", pri);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		log.info("Requested container ask: " + request.toString() + " Nodes" + Arrays.toString(nodes));
		writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
		return request;
	}

	public void evaluateReport(TaskInstance task, ContainerId containerId) {
		try {

			Data reportFile = new Data(Invocation.REPORT_FILENAME);
			reportFile.stageIn(fs, containerId.toString());
			Data stdoutFile = new Data(Invocation.STDOUT_FILENAME);
			stdoutFile.stageIn(fs, containerId.toString());
			Data stderrFile = new Data(Invocation.STDERR_FILENAME);
			stderrFile.stageIn(fs, containerId.toString());

			// (a) evaluate report
			Set<JsonReportEntry> report = task.getReport();
			try (BufferedReader reader = new BufferedReader(new FileReader(Invocation.REPORT_FILENAME))) {
				String line;
				while ((line = reader.readLine()) != null) {
					line = line.trim();
					if (line.isEmpty())
						continue;
					report.add(new JsonReportEntry(line));
				}
			}
			try (BufferedReader reader = new BufferedReader(new FileReader(Invocation.STDOUT_FILENAME))) {
				String line;
				StringBuffer sb = new StringBuffer();
				while ((line = reader.readLine()) != null) {
					sb.append(line.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\"")).append('\n');
				}
				String s = sb.toString();
				if (s.length() > 0) {
					JsonReportEntry re = new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(),
							task.getSignature(), null, JsonReportEntry.KEY_INVOC_STDOUT, sb.toString());
					report.add(re);
				}
			}
			try (BufferedReader reader = new BufferedReader(new FileReader(Invocation.STDERR_FILENAME))) {
				String line;
				StringBuffer sb = new StringBuffer();
				while ((line = reader.readLine()) != null) {
					sb.append(line.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\"")).append('\n');
				}
				String s = sb.toString();
				if (s.length() > 0) {
					JsonReportEntry re = new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(),
							task.getSignature(), null, JsonReportEntry.KEY_INVOC_STDERR, sb.toString());
					report.add(re);
				}
			}

		} catch (Exception e) {
			log.info("Error when attempting to evaluate report of invocation " + task.toString() + ". exiting");
			shutdown(e);
		}
	}

	public void writeEntryToLog(JsonReportEntry entry) {
		if (statLog.isDebugEnabled()) {
			statLog.debug(entry.toString());
		}
		scheduler.addEntryToDB(entry);
	}

	public void shutdown(Throwable t) {
		Writer writer = new StringWriter();
		PrintWriter printWriter = new PrintWriter(writer);
		t.printStackTrace(printWriter);
		log.error(writer.toString());
		done = true;
		amRMClient.stop();
	}

}
