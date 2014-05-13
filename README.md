Hi-WAY
======

<p>
The Heterogeneity-incorporating Workflow ApplicationMaster for YARN (Hi-WAY) provides the means to execute arbitrary
scientific workflows on top of <a href="http://hadoop.apache.org/">Apache's Hadoop 2.2.0 (YARN)</a>. In this context,
scientific workflows are directed acyclic graphs (DAGs), in which nodes are executables accessible from the command
line (e.g. tar, cat, or any other executable in the PATH of the worker nodes), and edges represent data dependencies
between these executables.
</p>

<p>
Hi-WAY currently supports the workflow languages <a
href="http://pegasus.isi.edu/wms/docs/latest/creating_workflows.php">Pegasus DAX</a> and <a
href="https://github.com/joergen7/cuneiform">Cuneiform</a> as well as the workflow schedulers static round robin,
HEFT, greedy queue and C3PO. Hi-WAY uses Hadoop's distributed file system HDFS to store the workflow's input, output
and intermediate data. The ApplicationMaster has been tested for up to 320 concurrent tasks and is fault-tolerant in
that it is able to restart failed tasks.
</p>

<p>
The Installation instructions can be found <a href="https://github.com/marcbux/Hi-WAY/wiki/Installation-Instructions">here</a>.
</p>
