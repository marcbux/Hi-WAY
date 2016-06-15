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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

	private final WorkflowDriver am;
	private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<>();

	public NMCallbackHandler(WorkflowDriver am) {
		this.am = am;
	}

	public void addContainer(ContainerId containerId, Container container) {
		containers.putIfAbsent(containerId, container);
	}

	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
		Container container = containers.get(containerId);
		if (container != null) {
			am.getNmClientAsync().getContainerStatusAsync(containerId, container.getNodeId());
		}
	}

	@Override
	public void onContainerStatusReceived(ContainerId arg0, ContainerStatus arg1) {
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		containers.remove(containerId);
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		System.err.println("Failed to query the status of Container " + containerId);
		t.printStackTrace();
		System.exit(-1);
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		System.err.println("Failed to start Container " + containerId);
		t.printStackTrace();
		containers.remove(containerId);
		am.getNumCompletedContainers().incrementAndGet();
		am.getNumFailedContainers().incrementAndGet();
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable t) {
		System.err.println("Failed to stop Container " + containerId);
		t.printStackTrace();
		containers.remove(containerId);
	}
}
