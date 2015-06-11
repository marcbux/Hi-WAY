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
package de.huberlin.wbi.hiway.am;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WFAppMetrics;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;
import de.huberlin.wbi.hiway.scheduler.Scheduler;
import de.huberlin.wbi.hiway.scheduler.c3po.C3PO;
import de.huberlin.wbi.hiway.scheduler.gq.GreedyQueue;
import de.huberlin.wbi.hiway.scheduler.heft.HEFT;
import de.huberlin.wbi.hiway.scheduler.rr.RoundRobin;

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
 * href="https://github.com/joergen7/cuneiform">Cuneiform</a> as well as the workflow schedulers static round robin, HEFT, greedy queue and C3PO. Hi-WAY uses
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
public abstract class HiWay {

	/**
	 * If the debug flag is set, dump out contents of current working directory and the environment to stdout for debugging.
	 */
	private static void dumpOutDebugInfo() {
		System.out.println("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
		}

		String cmd = "ls -al";
		Runtime run = Runtime.getRuntime();
		Process pr = null;
		try {
			pr = run.exec(cmd);
			pr.waitFor();

			try (BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()))) {
				String line = "";
				while ((line = buf.readLine()) != null) {
					System.out.println("System CWD content: " + line);
				}
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * The main routine.
	 * 
	 * @param appMaster
	 *            The Application Master
	 * @param args
	 *            Command line arguments passed to the ApplicationMaster.
	 */
	public static void loop(HiWay appMaster, String[] args) {
		boolean result = false;
		try {
			System.out.println("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			result = appMaster.run();
		} catch (Throwable t) {
			System.err.println("Error running ApplicationMaster");
			t.printStackTrace();
			System.exit(-1);
		}
		if (result) {
			System.out.println("Application Master completed successfully. exiting");
			System.exit(0);
		} else {
			System.out.println("Application Master failed. exiting");
			System.exit(2);
		}
	}

	/**
	 * Helper function to print usage.
	 * 
	 * @param opts
	 *            Parsed command line options.
	 */
	private static void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	private AMRMClientAsync.CallbackHandler allocListener;

	// the yarn tokens to be passed to any launched containers
	private ByteBuffer allTokens;
	// a handle to the YARN ResourceManager
	@SuppressWarnings("rawtypes")
	private AMRMClientAsync amRMClient;
	// this application's attempt id (combination of attemptId and fail count)
	private ApplicationAttemptId appAttemptID;
	// the internal id assigned to this application by the YARN ResourceManager
	private String appId;
	// the hostname of the container running the Hi-WAY ApplicationMaster
	private String appMasterHostname = "";
	// the port on which the ApplicationMaster listens for status updates from clients
	private int appMasterRpcPort = -1;
	// the tracking URL to which the ApplicationMaster publishes info for clients to monitor
	private String appMasterTrackingUrl = "";
	private HiWayConfiguration conf;
	private int containerCores = 1;
	// a listener for processing the responses from the NodeManagers
	private NMCallbackHandler containerListener;
	// the memory and number of virtual cores to request for the container on which the workflow tasks are launched
	private int containerMemory = 4096;
	private boolean determineFileSizes = false;
	// flags denoting workflow execution has finished and been successful
	private volatile boolean done;
	// the report, in which provenance information is stored
	private Data federatedReport;
	// private BufferedWriter federatedReportWriter;
	private Map<String, Data> files = new HashMap<>();
	// a handle to the hdfs
	private FileSystem hdfs;
	private Path hdfsApplicationDirectory;
	// a list of threads, one for each container launch
	private List<Thread> launchThreads = new ArrayList<>();
	// a structure that stores various metrics during workflow execution
	private final WFAppMetrics metrics = WFAppMetrics.create();
	// a handle to communicate with the YARN NodeManagers
	private NMClientAsync nmClientAsync;
	// a counter for allocated containers
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	// a counter for completed containers (complete denotes successful or failed
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	// a counter for failed containers
	private AtomicInteger numFailedContainers = new AtomicInteger();
	// a counter for killed containers
	private AtomicInteger numKilledContainers = new AtomicInteger();
	// a counter for requested containers
	private AtomicInteger numRequestedContainers = new AtomicInteger();
	// priority of the container request
	private int requestPriority;
	private UUID runId;
	// the workflow scheduler, as defined at workflow launch time
	private Scheduler scheduler;
	private HiWayConfiguration.HIWAY_SCHEDULER_OPTS schedulerName;
	// environment variables to be passed to any launched containers
	private Map<String, String> shellEnv = new HashMap<>();
	private BufferedWriter statLog;
	private volatile boolean success;
	private Path summaryPath;
	private Data workflowFile;

	private Path workflowPath;

	public HiWay() {
		conf = new HiWayConfiguration();
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		runId = UUID.randomUUID();
	}

	@SuppressWarnings("static-method")
	public void evaluateReport(TaskInstance task, ContainerId containerId) {
		try {
			Data reportFile = new Data(Invocation.REPORT_FILENAME, containerId.toString());
			reportFile.stageIn();
			Data stdoutFile = new Data(Invocation.STDOUT_FILENAME, containerId.toString());
			stdoutFile.stageIn();
			Data stderrFile = new Data(Invocation.STDERR_FILENAME, containerId.toString());
			stderrFile.stageIn();

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
					JsonReportEntry re = new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(),
							null, JsonReportEntry.KEY_INVOC_STDOUT, sb.toString());
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
					JsonReportEntry re = new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(),
							null, JsonReportEntry.KEY_INVOC_STDERR, sb.toString());
					report.add(re);
				}
			}

		} catch (Exception e) {
			System.out.println("Error when attempting to evaluate report of invocation " + task.toString() + ". exiting");
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private void finish() {
		writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_WF_TIME, Long.toString(System.currentTimeMillis()
				- amRMClient.getStartTime())));
		Collection<Data> outputFiles = getOutputFiles();
		if (outputFiles.size() > 0) {
			String outputs = getOutputFiles().toString();
			writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_WF_OUTPUT, outputs.substring(1, outputs.length() - 1)));
		}
		// Join all launched threads needed for when we time out and we need to release containers
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				System.err.println("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
				System.exit(-1);
			}
		}

		// When the application completes, it should stop all running containers
		System.out.println("Application completed. Stopping running containers");
		nmClientAsync.stop();

		// When the application completes, it should send a finish application signal to the RM
		System.out.println("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		success = true;

		System.out.println("Failed Containers: " + numFailedContainers.get());
		System.out.println("Completed Containers: " + numCompletedContainers.get());

		int numTotalContainers = scheduler.getNumberOfTotalTasks();

		System.out.println("Total Scheduled Containers: " + numTotalContainers);

		if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + numCompletedContainers.get() + ", allocated="
					+ numAllocatedContainers.get() + ", failed=" + numFailedContainers.get() + ", killed=" + numKilledContainers.get();
			success = false;
		}

		try {
			statLog.close();
			federatedReport.stageOut();
			if (summaryPath != null) {
				String stdout = hdfsApplicationDirectory + "/AppMaster.stdout";
				String stderr = hdfsApplicationDirectory + "/AppMaster.stderr";
				String statlog = hdfsApplicationDirectory + "/" + appId + ".log";

				try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryPath.toString()))) {
					JSONObject obj = new JSONObject();
					try {
						obj.put("output", getOutput());
						obj.put("stdout", stdout);
						obj.put("stderr", stderr);
						obj.put("statlog", statlog);
					} catch (JSONException e) {
						e.printStackTrace();
						System.exit(-1);
					}
					writer.write(obj.toString());
				}
				new Data("AppMaster.stdout").stageOut();
				new Data("AppMaster.stderr").stageOut();
				new Data(summaryPath).stageOut();
			}
		} catch (IOException e) {
			System.err.println("Error when attempting to stage out federated output log.");
			e.printStackTrace();
			System.exit(-1);
		}

		try {
			amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (YarnException | IOException e) {
			System.err.println("Failed to unregister application");
			e.printStackTrace();
			System.exit(-1);
		}

		amRMClient.stop();
	}

	public ByteBuffer getAllTokens() {
		return allTokens;
	}

	@SuppressWarnings("rawtypes")
	public AMRMClientAsync getAmRMClient() {
		return amRMClient;
	}

	public String getAppId() {
		return appId;
	}

	public HiWayConfiguration getConf() {
		return conf;
	}

	public NMCallbackHandler getContainerListener() {
		return containerListener;
	}

	public int getContainerMemory() {
		return containerMemory;
	}

	public Map<String, Data> getFiles() {
		return files;
	}

	public FileSystem getHdfs() {
		return hdfs;
	}

	public List<Thread> getLaunchThreads() {
		return launchThreads;
	}

	public WFAppMetrics getMetrics() {
		return metrics;
	}

	public NMClientAsync getNmClientAsync() {
		return nmClientAsync;
	}

	public AtomicInteger getNumAllocatedContainers() {
		return numAllocatedContainers;
	}

	public AtomicInteger getNumCompletedContainers() {
		return numCompletedContainers;
	}

	public AtomicInteger getNumFailedContainers() {
		return numFailedContainers;
	}

	public AtomicInteger getNumKilledContainers() {
		return numKilledContainers;
	}

	protected Collection<String> getOutput() {
		Collection<String> output = new ArrayList<>();
		for (Data outputFile : getOutputFiles()) {
			output.add(outputFile.getHdfsPath().toString());
		}
		return output;
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

	public UUID getRunId() {
		return runId;
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	public Map<String, String> getShellEnv() {
		return shellEnv;
	}

	public Data getWorkflowFile() {
		return workflowFile;
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
	 *             ParseException
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
		try {
			statLog = new BufferedWriter(new FileWriter(appId + ".log"));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}

		if (cliParser.hasOption("summary")) {
			summaryPath = new Path(cliParser.getOptionValue("summary"));
		}

		String hdfsBaseDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE, HiWayConfiguration.HIWAY_AM_DIRECTORY_BASE_DEFAULT);
		String hdfsSandboxDirectoryName = conf.get(HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE, HiWayConfiguration.HIWAY_AM_DIRECTORY_CACHE_DEFAULT);
		Path hdfsBaseDirectory = new Path(new Path(hdfs.getUri()), hdfsBaseDirectoryName);
		Data.setHdfsBaseDirectory(hdfsBaseDirectory);
		Path hdfsSandboxDirectory = new Path(hdfsBaseDirectory, hdfsSandboxDirectoryName);
		hdfsApplicationDirectory = new Path(hdfsSandboxDirectory, appId);
		Data.setHdfsApplicationDirectory(hdfsApplicationDirectory);
		Data.setHdfs(hdfs);

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

		System.out.println("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
				+ appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId=" + appAttemptID.getAttemptId());

		String shellEnvs[] = conf.getStrings(HiWayConfiguration.HIWAY_WORKER_SHELL_ENV, HiWayConfiguration.HIWAY_WORKER_SHELL_ENV_DEFAULT);
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

		if (!cliParser.hasOption("workflow")) {
			throw new IllegalArgumentException("No workflow file specified to be executed by application master");
		}

		workflowPath = new Path(cliParser.getOptionValue("workflow"));
		workflowFile = new Data(workflowPath);
		schedulerName = HiWayConfiguration.HIWAY_SCHEDULER_OPTS.valueOf(conf.get(HiWayConfiguration.HIWAY_SCHEDULER,
				HiWayConfiguration.HIWAY_SCHEDULER_DEFAULT.toString()));

		containerMemory = conf.getInt(HiWayConfiguration.HIWAY_WORKER_MEMORY, HiWayConfiguration.HIWAY_WORKER_MEMORY_DEFAULT);
		containerCores = conf.getInt(HiWayConfiguration.HIWAY_WORKER_VCORES, HiWayConfiguration.HIWAY_WORKER_VCORES_DEFAULT);
		requestPriority = conf.getInt(HiWayConfiguration.HIWAY_WORKER_PRIORITY, HiWayConfiguration.HIWAY_WORKER_PRIORITY_DEFAULT);
		return true;
	}

	public boolean isDetermineFileSizes() {
		return determineFileSizes;
	}

	public abstract void parseWorkflow();

	/**
	 * Main run function for the application master
	 * 
	 * @return True if there were no errors
	 * @throws YarnException
	 *             YarnException
	 * @throws IOException
	 *             IOException
	 */
	@SuppressWarnings("unchecked")
	public boolean run() throws YarnException, IOException {
		System.out.println("Starting ApplicationMaster");

		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		try (DataOutputBuffer dob = new DataOutputBuffer()) {
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

			allocListener = new RMCallbackHandler(this);
			amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
			amRMClient.init(conf);
			amRMClient.start();

			containerListener = new NMCallbackHandler(this);
			nmClientAsync = new NMClientAsyncImpl(containerListener);
			nmClientAsync.init(conf);
			nmClientAsync.start();

			Data workflowData = new Data(workflowPath);
			workflowData.stageIn();

			// Register self with ResourceManager. This will start heartbeating to the RM.
			appMasterHostname = NetUtils.getHostname();
			RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

			switch (schedulerName) {
			case staticRoundRobin:
			case heft:
				scheduler = schedulerName.equals(HiWayConfiguration.HIWAY_SCHEDULER_OPTS.staticRoundRobin) ? new RoundRobin(getWorkflowName(), hdfs, conf)
						: new HEFT(getWorkflowName(), hdfs, conf);
				break;
			case greedyQueue:
				scheduler = new GreedyQueue(getWorkflowName(), conf, hdfs);
				break;
			default:
				C3PO c3po = new C3PO(getWorkflowName(), hdfs, conf);
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
			federatedReport = new Data(appId + ".log");

			// Dump out information about cluster capability as seen by the resource manager
			int maxMem = response.getMaximumResourceCapability().getMemory();
			int maxCores = response.getMaximumResourceCapability().getVirtualCores();
			System.out.println("Max mem capabililty of resources in this cluster " + maxMem);

			// A resource ask cannot exceed the max.
			if (containerMemory > maxMem) {
				System.out.println("Container memory specified above max threshold of cluster." + " Using max value." + ", specified=" + containerMemory
						+ ", max=" + maxMem);
				containerMemory = maxMem;
			}
			if (containerCores > maxCores) {
				System.out.println("Container vcores specified above max threshold of cluster." + " Using max value." + ", specified=" + containerCores
						+ ", max=" + maxCores);
				containerCores = maxCores;
			}

			while (!done) {
				try {
					while (scheduler.hasNextNodeRequest()) {
						ContainerRequest containerAsk = setupContainerAskForRM(scheduler.getNextNodeRequest());
						amRMClient.addContainerRequest(containerAsk);
					}
					Thread.sleep(1000);
					System.out.println("Current application state: requested=" + numRequestedContainers + ", completed=" + numCompletedContainers + ", failed="
							+ numFailedContainers + ", killed=" + numKilledContainers + ", allocated=" + numAllocatedContainers);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
			finish();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return success;
	}

	public void setDetermineFileSizes() {
		determineFileSizes = true;
	}

	public void setDone() {
		this.done = true;
	}

	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 * 
	 * @param nodes
	 *            The worker nodes on which this container is to be allocated. If left empty, the container will be launched on any worker node fulfilling the
	 *            resource requirements.
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
			System.exit(-1);
		}

		System.out.println("Requested container ask: " + request.toString() + " Nodes" + Arrays.toString(nodes));
		writeEntryToLog(new JsonReportEntry(getRunId(), null, null, null, null, null, HiwayDBI.KEY_HIWAY_EVENT, value));
		return request;
	}

	@SuppressWarnings("static-method")
	public void taskFailure(TaskInstance task, ContainerId containerId) {
		String line;

		try {
			System.err.println("[script]");
			try (BufferedReader reader = new BufferedReader(new StringReader(task.getCommand()))) {
				int i = 0;
				while ((line = reader.readLine()) != null)
					System.err.println(String.format("%02d  %s", Integer.valueOf(++i), line));
			}

			Data stdoutFile = new Data(Invocation.STDOUT_FILENAME, containerId.toString());
			stdoutFile.stageIn();

			System.err.println("[out]");
			try (BufferedReader reader = new BufferedReader(new FileReader(stdoutFile.getLocalPath().toString()))) {
				while ((line = reader.readLine()) != null)
					System.err.println(line);
			}

			Data stderrFile = new Data(Invocation.STDERR_FILENAME, containerId.toString());
			stderrFile.stageIn();

			System.err.println("[err]");
			try (BufferedReader reader = new BufferedReader(new FileReader(stderrFile.getLocalPath().toString()))) {
				while ((line = reader.readLine()) != null)
					System.err.println(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		System.err.println("[end]");
	}

	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		try {
			for (TaskInstance childTask : task.getChildTasks()) {
				if (childTask.readyToExecute())
					scheduler.addTaskToQueue(childTask);
			}
		} catch (WorkflowStructureUnknownException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		for (Data data : task.getOutputData()) {
			data.setContainerId(containerId.toString());
		}
		if (scheduler.getNumberOfReadyTasks() == 0 && scheduler.getNumberOfRunningTasks() == 0) {
			done = true;
		}
	}

	public void writeEntryToLog(JsonReportEntry entry) {
		try {
			statLog.write(entry.toString());
			statLog.newLine();
			statLog.flush();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		scheduler.addEntryToDB(entry);
	}

}
