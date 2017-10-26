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

import de.huberlin.wbi.hiway.common.TaskInstance;

public class HiWayInvocation {
	public final TaskInstance task;
	public final long timestamp;

	public HiWayInvocation(TaskInstance task) {
		this.task = task;
		timestamp = System.currentTimeMillis();
	}
}
