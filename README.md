Hi-WAY
======

<p>
The <b>Hi</b>-WAY <b>W</b>orkflow <b>A</b>pplicationMaster for <b>Y</b>ARN provides the means to execute arbitrary scientific workflows on top of <a href="http://hadoop.apache.org/">Apache Hadoop 2 (YARN)</a>.
In this context, scientific workflows are directed acyclic graphs (DAGs), in which nodes are black-box tasks (e.g. Bash scripts, Java programs, Python scripts, compiled C++ executables) processing unstructured data (arbitrary files).
Edges in the graph represent data dependencies between the tasks.
Hi-WAY uses Hadoop's distributed file system HDFS to store the workflow's input, output and intermediate data.
</p>

<p>
Hi-WAY currently supports the workflow languages <a href="https://github.com/joergen7/cuneiform">Cuneiform</a>, <a href="https://usegalaxy.org/">Galaxy</a>, and <a href="http://pegasus.isi.edu/wms/docs/latest/creating_workflows.php">Pegasus DAX</a>, yet can be easily extended to support other workflow languages.
A number of general-purpose and more specialized schedulers are provided, which can take into account data locality when assigning tasks to machines to reduce data transfer times during workflow execution.
When running workflows, Hi-WAY captures comprehensive provenance information, which can be stored as files in HDFS, as well as in a MySQL or Couchbase database.
These provenance traces are evaluated by the scheduler for performance estimation and can be used to re-execute previous workflow runs.
The ApplicationMaster has been tested to scale to more than 600 concurrent tasks and is fault-tolerant in that it is able to restart failed tasks.
</p>

<p>
The Installation instructions can be found <a href="https://github.com/marcbux/Hi-WAY/wiki/Installation-Instructions">here</a>.
</p>
