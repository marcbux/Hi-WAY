package de.huberlin.wbi.hiway.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

import de.huberlin.wbi.cuneiform.core.cre.BaseCreActor;
import de.huberlin.wbi.cuneiform.core.cre.TicketReadyMsg;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.repl.Repl;
import de.huberlin.wbi.cuneiform.core.semanticmodel.CompoundExpr;
import de.huberlin.wbi.cuneiform.core.semanticmodel.NotDerivableException;
import de.huberlin.wbi.cuneiform.core.semanticmodel.Ticket;
import de.huberlin.wbi.cuneiform.core.ticketsrc.TicketSrcActor;
import de.huberlin.wbi.hiway.common.StaticTaskInstance;
import de.huberlin.wbi.hiway.common.Constant;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class CuneiformApplicationMaster extends AbstractApplicationMaster {

	public class CuneiformTaskInstance extends StaticTaskInstance {

		private Invocation invocation;

		public CuneiformTaskInstance(Invocation invocation) {
			super(invocation.getRunId(), invocation.getTaskName(), invocation
					.getTaskId(), invocation.getLangLabel());
			this.invocation = invocation;
		}

		public Invocation getInvocation() {
			return invocation;
		}

		@Override
		public boolean readyToExecute() {
			return true;
		}
		
		@Override
		public void addChildTask(TaskInstance childTask)
				throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

		@Override
		public void addParentTask(TaskInstance parentTask)
				throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

		@Override
		public Set<TaskInstance> getChildTasks()
				throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

		@Override
		public int getDepth() throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

		@Override
		public Set<TaskInstance> getParentTasks()
				throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

		@Override
		public double getUpwardRank() throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

		@Override
		public void setDepth(int depth) throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

		@Override
		public void setUpwardRank(double upwardRank)
				throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException(
					"Workflow structure not derivable in Cuneiform");
		}

	}

	// Cre - Cuneiform Runtime Environment
	public class HiWayCreActor extends BaseCreActor {

		@Override
		public void processMsg(TicketReadyMsg msg) {

			Ticket ticket = msg.getTicket();
			Invocation invoc = Invocation.createInvocation(ticket);
			TaskInstance task = new CuneiformTaskInstance(invoc);

			try {
				task.setSignature(invoc.getTicketId());
				
				for (String inputName : invoc.getStageInList()) {

					if (!files.containsKey(inputName)) {
						Data data = new Data(inputName);
						data.setInput(true);
						files.put(inputName, data);
					}
					Data data = files.get(inputName);
					task.addInputData(data);
				}
			} catch (NotDerivableException e) {
				e.printStackTrace();
			}

			task.setCommand("./" + CUNEIFORM_SCRIPT_FILENAME);
			scheduler.addTask(task);
		}

		@Override
		protected void shutdown() {
			done = true;
		}

	}

	// Repl - Read evaluation print loop
	public class HiWayRepl extends Repl {

		public HiWayRepl(TicketSrcActor ticketSrc) {
			super(ticketSrc);
		}

		@Override
		public synchronized void runFinished(UUID runId, CompoundExpr result) {
			super.runFinished(runId, result);
			try {
				for (String output : result.normalize()) {
					if (files.containsKey(output)) {
						files.get(output).setOutput(true);
					}
				}
			} catch (NotDerivableException e) {
				e.printStackTrace();
			}
		}

	}

	private TicketSrcActor ticketSrc;
	
	private static final String CUNEIFORM_SCRIPT_FILENAME = "__cuneiform_script__";

	private static final Log log = LogFactory
			.getLog(CuneiformApplicationMaster.class);

	public static void main(String[] args) {
		AbstractApplicationMaster.loop(new CuneiformApplicationMaster(), args);
	}

	// private Map<Data, CuneiformTaskInstance> fileToProducer;

	// private Map<CuneiformTaskInstance, Invocation> taskToInvocation;

	public CuneiformApplicationMaster() {
		super();
		// taskToInvocation = new HashMap<>();
		// fileToProducer = new HashMap<>();
	}

	@Override
	protected void buildPostScript(TaskInstance task, Container container)
			throws IOException {
		File postScript = new File(Constant.POST_SCRIPT_FILENAME);
		BufferedWriter postScriptWriter = new BufferedWriter(new FileWriter(
				postScript));
		postScriptWriter.write(Constant.BASH_SHEBANG);
		String[] containerFiles = { Constant.SUPER_SCRIPT_FILENAME,
				Constant.PRE_SCRIPT_FILENAME, CUNEIFORM_SCRIPT_FILENAME,
				Constant.POST_SCRIPT_FILENAME };

		postScriptWriter
				.write("for file in $( find * -type l \\( ! -path 'tmp/*'");
		for (String containerFile : containerFiles) {
			postScriptWriter.write(" -a ! -path '" + containerFile + "'");
		}
		// for (Data data : task.getInputData()) {
		// postScriptWriter.write(" -a ! -path '" + data.getLocalPath() + "'");
		// }
		postScriptWriter.write(" \\) )\ndo\n");

		String timeString = generateTimeString(task,
				Constant.KEY_FILE_TIME_STAGEOUT)
				+ "hdfs dfs -copyFromLocal -f $file "
				+ Data.getHdfsDirectoryPrefix()
				+ "/"
				+ container.getId().toString() + "/$file &";
		postScriptWriter
				.write("\tif [ `dirname $file` != '.' ]\n\tthen\n\t\thdfs dfs -mkdir -p "
						+ Data.getHdfsDirectoryPrefix()
						+ "/"
						+ container.getId().toString()
						+ "/`dirname $file` && "
						+ timeString
						+ "\n\telse\n\t\t"
						+ timeString
						+ "\n\tfi\n");

		postScriptWriter
				.write("\twhile [ $(jobs -p "
						+ /* 2>&1 */"| grep -c \\[0-9\\]\\*) -ge "
						+ hdfsInstancesPerContainer
						+ " ]\n\tdo\n\t\tsleep 1\n\tdone\n");

		postScriptWriter.write("done\n");

		postScriptWriter.write("for job in `jobs -p`\ndo\n\twait $job\ndone\n");
		postScriptWriter.close();
		task.addScript(new Data(postScript.getPath()));
	}

	@Override
	protected void buildSuperScript(TaskInstance task, Container container)
			throws IOException {
		super.buildSuperScript(task, container);
		File cuneiformScript = new File(CUNEIFORM_SCRIPT_FILENAME);
		BufferedWriter cuneiformScriptWriter = new BufferedWriter(
				new FileWriter(cuneiformScript));
		try {
			cuneiformScriptWriter.write(((CuneiformTaskInstance) task)
					.getInvocation().toScript());
		} catch (Exception e) {
			log.info("Error when attempting to write Cuneiform script for task "
					+ task.toString() + " to file. exiting");
			e.printStackTrace();
			System.exit(1);
		}
		cuneiformScriptWriter.close();
		Data script = new Data(cuneiformScript.getPath());
		// task.setSuperScript(script);
		task.addScript(script);
	}

	@Override
	public String getRunId() {
		return (ticketSrc.getRunId().toString());
	}

	@Override
	public void parseWorkflow() {
		ExecutorService executor = Executors.newCachedThreadPool();

		BaseCreActor creActor = new HiWayCreActor();
		executor.submit(creActor);

		ticketSrc = new TicketSrcActor(creActor);
		executor.submit(ticketSrc);
		executor.shutdown();

		Repl repl = new HiWayRepl(ticketSrc);

		StringBuffer buf = new StringBuffer();

		try {
			try (BufferedReader reader = new BufferedReader(new FileReader(
					new File(workflowFile.getLocalPath())))) {
				String line;
				while ((line = reader.readLine()) != null) {
					buf.append(line).append('\n');
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		repl.interpret(buf.toString());
	}

	@Override
	public void taskFailure(TaskInstance task, ContainerId containerId) {
		String line;
		try (BufferedReader reader = new BufferedReader(new StringReader(
				((CuneiformTaskInstance) task).getInvocation().toScript()))) {
			System.err.println("[script]");
			int i = 0;
			while ((line = reader.readLine()) != null)
				System.err.println(String.format("%02d  %s", ++i, line));
		} catch (Exception e) {
			e.printStackTrace();
		}
		super.taskFailure(task, containerId);
	}

	@Override
	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		super.taskSuccess(task, containerId);

		try {
			Invocation invocation = ((CuneiformTaskInstance) task)
					.getInvocation();
			invocation.evalReport(task.getReport());

			// (b) set output files
			for (String outputName : invocation.getStageOutList()) {
				if (!files.containsKey(outputName)) {
					Data output = new Data(outputName);
					files.put(outputName, output);
					// fileToProducer.put(output, task);
				}
				Data output = files.get(outputName);
				Data.hdfsDirectoryMidfixes.put(output, containerId.toString());

				task.addOutputData(output);
				output.setInput(false);
				// if (terminalTaskNodes.contains(invocation.getTaskNode())) {
				// output.setOutput(true);
				// }
			}

		} catch (Exception e) {
			log.info("Error when attempting to evaluate report of invocation "
					+ task.toString() + ". exiting");
			e.printStackTrace();
			System.exit(1);
		}
	}

}
