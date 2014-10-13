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
package de.huberlin.wbi.hiway.app.am;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.json.JSONException;

import de.huberlin.wbi.cuneiform.core.cre.BaseCreActor;
import de.huberlin.wbi.cuneiform.core.cre.TicketReadyMsg;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.repl.BaseRepl;
import de.huberlin.wbi.cuneiform.core.semanticmodel.CompoundExpr;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.cuneiform.core.semanticmodel.NotBoundException;
import de.huberlin.wbi.cuneiform.core.semanticmodel.NotDerivableException;
import de.huberlin.wbi.cuneiform.core.semanticmodel.Ticket;
import de.huberlin.wbi.cuneiform.core.ticketsrc.TicketFailedMsg;
import de.huberlin.wbi.cuneiform.core.ticketsrc.TicketFinishedMsg;
import de.huberlin.wbi.cuneiform.core.ticketsrc.TicketSrcActor;
import de.huberlin.wbi.hiway.app.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class CuneiformApplicationMaster extends HiWay {

	public class CuneiformTaskInstance extends TaskInstance {

		private Invocation invocation;

		public CuneiformTaskInstance(Invocation invocation) {
			super(invocation.getTicketId(), invocation.getRunId(), invocation.getTaskName(), invocation.getTaskId(), invocation.getLangLabel());
			this.invocation = invocation;
		}

		@Override
		public void addChildTask(TaskInstance childTask) throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
		}

		@Override
		public void addParentTask(TaskInstance parentTask) throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
		}

		@Override
		public Set<TaskInstance> getChildTasks() throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
		}

		@Override
		public int getDepth() throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
		}

		public Invocation getInvocation() {
			return invocation;
		}

		@Override
		public Set<TaskInstance> getParentTasks() throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
		}

		@Override
		public double getUpwardRank() throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
		}

		@Override
		public boolean readyToExecute() {
			return true;
		}

		@Override
		public void setDepth(int depth) throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
		}

		@Override
		public void setUpwardRank(double upwardRank) throws WorkflowStructureUnknownException {
			throw new WorkflowStructureUnknownException("Workflow structure not derivable in Cuneiform");
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
				onError(e);
			}

			try {
				task.setCommand(invoc.toScript());
				writeEntryToLog(invoc.getExecutableLogEntry());
				writeEntryToLog(invoc.getScriptLogEntry());
			} catch (NotBoundException | NotDerivableException e) {
				onError(e);
			}

			Collection<TaskInstance> tasks = new ArrayList<>();
			tasks.add(task);
			scheduler.addTasks(tasks);
		}

		@Override
		protected void shutdown() {
		}

	}

	// Repl - Read evaluation print loop
	public class HiWayRepl extends BaseRepl {

		public HiWayRepl(TicketSrcActor ticketSrc) {
			super(ticketSrc);
		}

		@Override
		protected void flushStatLog(Set<JsonReportEntry> reportEntrySet) {
		}

		@Override
		public void queryFailedPost(UUID queryId, Long ticketId, Exception e, String script, String stdOut, String stdErr) {
			log.info("Query failed.");
			done = true;
		}

		@Override
		public void queryFinishedPost(UUID queryId, CompoundExpr result) {
			log.info("Query finished.");
			done = true;
			try {
				for (String output : result.normalize()) {
					if (files.containsKey(output)) {
						files.get(output).setOutput(true);
					}
				}
			} catch (NotDerivableException e) {
				onError(e);
			}
		}

		@Override
		public void queryStartedPost(UUID runId) {
		}

	}

	private static final Log log = LogFactory.getLog(CuneiformApplicationMaster.class);

	public static void main(String[] args) {
		HiWay.loop(new CuneiformApplicationMaster(), args);
	}

	private BaseCreActor creActor;

	private final TicketSrcActor ticketSrc;

	public CuneiformApplicationMaster() {
		super();
		ExecutorService executor = Executors.newCachedThreadPool();

		creActor = new HiWayCreActor();
		executor.submit(creActor);

		ticketSrc = new TicketSrcActor(creActor);
		executor.submit(ticketSrc);
		executor.shutdown();
	}

	@Override
	public UUID getRunId() {
		return (ticketSrc.getRunId());
	}

	@Override
	public void parseWorkflow() {
		log.info("Parsing Cuneiform workflow " + workflowFile);
		BaseRepl repl = new HiWayRepl(ticketSrc);

		StringBuffer buf = new StringBuffer();

		try {
			try (BufferedReader reader = new BufferedReader(new FileReader(new File(workflowFile.getLocalPath())))) {
				String line;
				while ((line = reader.readLine()) != null) {
					buf.append(line).append('\n');
				}
			}
		} catch (FileNotFoundException e) {
			onError(e);
		} catch (IOException e) {
			onError(e);
		}
		repl.interpret(buf.toString());
	}
	
	@Override
	public void taskFailure(TaskInstance task, ContainerId containerId) {
		super.taskFailure(task, containerId);
		
		String line;
		try {
			StringBuffer buf = new StringBuffer();
			try (BufferedReader reader = new BufferedReader(new FileReader(new File(Invocation.STDOUT_FILENAME)))) {
				while ((line = reader.readLine()) != null)
					buf.append(line).append('\n');
			}
			String stdOut = buf.toString();
			
			buf = new StringBuffer();
			try (BufferedReader reader = new BufferedReader(new FileReader(new File(Invocation.STDERR_FILENAME)))) {
				while ((line = reader.readLine()) != null)
					buf.append(line).append('\n');
			}
			String stdErr = buf.toString();
			Invocation invocation = ((CuneiformTaskInstance) task).getInvocation();
			if (!task.retry(hiWayConf.getInt(HiWayConfiguration.HIWAY_AM_TASK_RETRIES, HiWayConfiguration.HIWAY_AM_TASK_RETRIES_DEFAULT))) {
				ticketSrc.sendMsg(new TicketFailedMsg(creActor, invocation.getTicket(), null, task.getCommand(), stdOut, stdErr));
			}			
		} catch (IOException e) {
			onError(e);
		}
	}
	
	@Override
	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		try {
			Invocation invocation = ((CuneiformTaskInstance) task).getInvocation();
			invocation.evalReport(task.getReport());
			ticketSrc.sendMsg(new TicketFinishedMsg(creActor, invocation.getTicket(), task.getReport()));
			log.info("Message sent.");

			// set output files
			for (String outputName : invocation.getStageOutList()) {
				if (!files.containsKey(outputName)) {
					Data output = new Data(outputName);
					files.put(outputName, output);
				}
				Data output = files.get(outputName);
				Data.hdfsDirectoryMidfixes.put(output, containerId.toString());

				task.addOutputData(output);
				output.setInput(false);
			}

		} catch (JSONException | NotDerivableException e) {
			log.info("Error when attempting to evaluate report of invocation " + task.toString() + ". exiting");
			onError(e);
		}
	}
	
}
