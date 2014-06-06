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
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.AbstractTaskInstance;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.Parser;
import edu.isi.pegasus.planner.parser.dax.DAX2CDAG;
import edu.isi.pegasus.planner.parser.dax.DAXParser;

public class DaxApplicationMaster extends AbstractApplicationMaster {

	// protected class DaxRMCallbackHandler extends RMCallbackHandler {
	//
	// @Override
	// public void onContainersCompleted(
	// List<ContainerStatus> completedContainers) {
	// super.onContainersCompleted(completedContainers);
	//
	// else {
	// // ask for more containers if new tasks are available or containers have
	// failed
	// while (scheduler.hasNextNodeRequest()) {
	// numRequestedContainers.incrementAndGet();
	// ContainerRequest containerAsk =
	// setupContainerAskForRM(scheduler.getNextNodeRequest());
	// amRMClient.addContainerRequest(containerAsk);
	// }
	// }
	// }
	// }

	private static final Log log = LogFactory
			.getLog(DaxApplicationMaster.class);

	public static void main(String[] args) {
		AbstractApplicationMaster.loop(new DaxApplicationMaster(), args);
	}

	private ADag dag;

	public DaxApplicationMaster() {
		super();
	}

	@Override
	public String getRunId() {
		return dag.getWorkflowUUID();
	}

	@Override
	public void parseWorkflow() {
		Map<Object, TaskInstance> tasks = new HashMap<>();

		log.info("Parsing Pegasus DAX " + workflowFile);

		PegasusProperties properties = PegasusProperties.nonSingletonInstance();
		PegasusBag bag = new PegasusBag();
		bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

		LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
		logger.logEventStart("DaxWorkflow", "", "");
		logger.setLevel(5);
		bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);

		DAXParser daxParser = DAXParserFactory.loadDAXParser(bag, "DAX2CDAG",
				workflowFile.getLocalPath());
		((Parser) daxParser).startParser(workflowFile.getLocalPath());
		dag = (ADag) ((DAX2CDAG) daxParser.getDAXCallback())
				.getConstructedObject();

		log.info("Generating Workflow " + dag.getAbstractWorkflowName());

		Queue<String> jobQueue = new LinkedList<>();
		for (Object rootNode : dag.getRootNodes()) {
			jobQueue.add((String) rootNode);
		}

		while (!jobQueue.isEmpty()) {
			String jobName = jobQueue.remove();
			Job job = dag.getSubInfo(jobName);
			String taskId = job.getID();

			String taskName = job.getTXName();
			TaskInstance task = new AbstractTaskInstance(UUID.fromString(dag
					.getWorkflowUUID()), taskName,
					Math.abs(taskName.hashCode()));
			task.setSignature(Math.abs(taskId.hashCode()));
			tasks.put(taskId, task);

			for (Object input : job.getInputFiles()) {
				String inputName = ((PegasusFile) input).getLFN();
				if (!files.containsKey(inputName)) {
					Data data = new Data(inputName);
					data.setInput(true);
					files.put(inputName, data);
				}
				Data data = files.get(inputName);
				task.addInputData(data);
			}

			if (job.getOutputFiles().size() > 0) {

				List<String> outputs = new LinkedList<>();

				for (Object output : job.getOutputFiles()) {
					String outputName = ((PegasusFile) output).getLFN();
					if (!files.containsKey(outputName))
						files.put(outputName, new Data(outputName));
					Data data = files.get(outputName);
					task.addOutputData(data);
					data.setInput(false);
					outputs.add(outputName);
				}

				try {
					task.getReport().add(
							new JsonReportEntry(task.getWorkflowId(), task
									.getTaskId(), task.getTaskName(), task
									.getLanguageLabel(), task.getSignature(),
									null, JsonReportEntry.KEY_INVOC_OUTPUT,
									new JSONObject().put("output", outputs)));
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}

			for (Object parent : dag.getParents(jobName)) {
				TaskInstance parentTask = tasks.get(parent);
				try {
					task.addParentTask(parentTask);
					parentTask.addChildTask(task);
				} catch (WorkflowStructureUnknownException e) {
					throw new RuntimeException(e);
				}
			}

			for (Object child : dag.getChildren(jobName)) {
				String childName = (String) child;
				if (!tasks.containsKey(child)
						&& tasks.keySet()
								.containsAll(dag.getParents(childName)))
					jobQueue.add(childName);
			}

			log.info("Adding task " + task + ": " + task.getInputData()
					+ " -> " + task.getOutputData());

			task.setCommand(taskName + job.getArguments().replaceAll(" +", " "));
		}

		for (TaskInstance task : tasks.values()) {
			try {
				if (task.getChildTasks().size() == 0) {
					for (Data data : task.getOutputData()) {
						data.setOutput(true);
					}
				}
			} catch (WorkflowStructureUnknownException e) {
				throw new RuntimeException(e);
			}
		}

		scheduler.addTasks(tasks.values());
	}

	@Override
	public void taskFailure(TaskInstance task, ContainerId containerId) {
		log.error("[script]");
		log.error(task.getCommand());
		String line;

		try {
			Data stdoutFile = new Data(Invocation.STDOUT_FILENAME);
			stdoutFile.stageIn(fs, containerId.toString());

			log.error("[out]");
			try (BufferedReader reader = new BufferedReader(new FileReader(
					new File(stdoutFile.getLocalPath())))) {
				while ((line = reader.readLine()) != null)
					log.error(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			Data stderrFile = new Data(Invocation.STDERR_FILENAME);
			stderrFile.stageIn(fs, containerId.toString());

			log.error("[err]");
			try (BufferedReader reader = new BufferedReader(new FileReader(
					new File(stderrFile.getLocalPath())))) {
				while ((line = reader.readLine()) != null)
					log.error(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		log.error("[end]");
	}

	@Override
	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		super.taskSuccess(task, containerId);
		try {
			for (TaskInstance childTask : task.getChildTasks()) {
				if (childTask.readyToExecute())
					scheduler.addTaskToQueue(childTask);
			}
		} catch (WorkflowStructureUnknownException e) {
			throw new RuntimeException(e);
		}
		for (Data data : task.getOutputData()) {
			Data.hdfsDirectoryMidfixes.put(data, containerId.toString());
		}
		if (scheduler.getNumberOfReadyTasks() == 0
				&& scheduler.getNumberOfRunningTasks() == 0) {
			done = true;
		}
	}

}
