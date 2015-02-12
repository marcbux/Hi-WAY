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
package de.huberlin.wbi.hiway.am;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import de.huberlin.wbi.hiway.am.NMCallbackHandler;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * Thread to connect to the {@link ContainerManagementProtocol} and launch the container that will execute the shell command.
 */
public class LaunchContainerRunnable implements Runnable {

	private HiWay am;
	private Container container;
	private NMCallbackHandler containerListener;
	private TaskInstance task;

	/**
	 * @param lcontainer
	 *            Allocated container
	 * @param containerListener
	 *            Callback handler of the container
	 */
	public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener, TaskInstance task, HiWay am) {
		this.container = lcontainer;
		this.containerListener = containerListener;
		this.task = task;
		this.am = am;
	}

	/**
	 * Connects to CM, sets up container launch context for shell command and eventually dispatches the container start request to the CM.
	 */
	@Override
	public void run() {
		System.out.println("Setting up container launch container for containerid=" + container.getId());
		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

		// Set the environment
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*");
		for (String c : am.getConf().getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(':');
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}

		if (am.getConf().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
			classPathEnv.append(':');
			classPathEnv.append(System.getProperty("java.class.path"));
		}

		am.getShellEnv().put("CLASSPATH", classPathEnv.toString());

		// Set the environment
		ctx.setEnvironment(am.getShellEnv());

		ctx.setLocalResources(task.buildScriptsAndSetResources(am.getFs(), container));

		// Set the necessary command to execute on the allocated container
		Vector<CharSequence> vargs = new Vector<>(5);

		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		// Set Xmx based on am memory size
		vargs.add("-Xmx" + am.getContainerMemory() + "m");
		// Set class name
		vargs.add(HiWayConfiguration.HIWAY_WORKER_CLASS);

		vargs.add("--appId " + am.getAppId().toString());
		vargs.add("--containerId " + container.getId().toString());
		vargs.add("--workflowId " + task.getWorkflowId());
		vargs.add("--taskId " + task.getTaskId());
		vargs.add("--taskName " + task.getTaskName());
		vargs.add("--langLabel " + task.getLanguageLabel());
		vargs.add("--id " + task.getId());
		for (Data inputData : task.getInputData()) {
			vargs.add("--input " + inputData.getLocalPath() + "," + inputData.isInput() + "," + inputData.getContainerId());
		}
		for (Data outputData : task.getOutputData()) {
			vargs.add("--output " + outputData.getLocalPath());
		}
		if (am.isDetermineFileSizes()) {
			vargs.add("--size");
		}

		String invocScript = task.getInvocScript();
		if (invocScript.length() > 0) {
			vargs.add("--invocScript " + invocScript);
		}

		// Get final commmand
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		List<String> commands = new ArrayList<>();
		commands.add(command.toString());
		ctx.setCommands(commands);

		/* Set up tokens for the container. For normal shell commands, the container in distribute-shell doesn't need any tokens. We are populating them mainly
		 * for NodeManagers to be able to download any files in the distributed file-system. The tokens are otherwise also useful in cases, for e.g., when one
		 * is running a "hadoop dfs" command inside the distributed shell. */
		ctx.setTokens(am.getAllTokens().duplicate());

		containerListener.addContainer(container.getId(), container);
		am.getNmClientAsync().startContainerAsync(container, ctx);
	}
}
