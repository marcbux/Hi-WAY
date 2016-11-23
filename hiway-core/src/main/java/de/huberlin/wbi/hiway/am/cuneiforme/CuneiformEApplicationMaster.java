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
package de.huberlin.wbi.hiway.am.cuneiforme;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.wbi.cfjava.cuneiform.HaltMsg;
import de.huberlin.wbi.cfjava.cuneiform.RemoteWorkflow;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

public class CuneiformEApplicationMaster extends WorkflowDriver {

	public static void main(String[] args) {
		WorkflowDriver.launch(new CuneiformEApplicationMaster(), args);
	}

	private RemoteWorkflow workflow;
	private Map<CuneiformETaskInstance, JSONObject> requests;

	public CuneiformEApplicationMaster() {
		super();
		requests = new HashMap<>();
		setDetermineFileSizes();
	}

	@Override
	public Collection<TaskInstance> parseWorkflow() {
		try (BufferedReader reader = new BufferedReader(new FileReader(getWorkflowFile().getLocalPath().toString()))) {
			WorkflowDriver.writeToStdout("Parsing Cuneiform workflow " + getWorkflowFile());
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}

			String ip = getConf().get(HiWayConfiguration.HIWAY_WORKFLOW_LANGUAGE_CUNEIFORME_SERVER_IP);
			WorkflowDriver.writeToStdout("Connecting to Cuneiform server at " + ip);
			workflow = new RemoteWorkflow(sb.toString(), ip);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		return getNextTasks();
	}

	@Override
	public boolean isDone() {
		return !workflow.isRunning();
	}

	@Override
	protected void loop() {
		try {
			workflow.update();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		getScheduler().addTasks(getNextTasks());
		super.loop();
	}

	@Override
	protected void finish() {
		HaltMsg haltMsg = workflow.getHaltMsg();
		if (haltMsg.isOk()) {
		} else if (haltMsg.isErrorTask()) {
			WorkflowDriver.writeToStdout("Workflow execution halted due to task failure.");
			WorkflowDriver.writeToStdout("app line: " + haltMsg.getAppLine());
			WorkflowDriver.writeToStdout("lam name: " + haltMsg.getLamName());
			WorkflowDriver.writeToStdout("output:   " + haltMsg.getOutput());
			WorkflowDriver.writeToStdout("script:   " + haltMsg.getScript());
		} else if (haltMsg.isErrorWorkflow()) {
			WorkflowDriver.writeToStdout("Workflow execution halted due to workflow failure.");
			WorkflowDriver.writeToStdout("line:   " + haltMsg.getLine());
			WorkflowDriver.writeToStdout("module: " + haltMsg.getModule());
			WorkflowDriver.writeToStdout("reason: " + haltMsg.getReason());
		} else {
			throw new UnsupportedOperationException();
		}

		super.finish();
	}

	@Override
	protected Collection<String> getOutput() {
		Collection<String> outputs = new LinkedList<>();
		HaltMsg haltMsg = workflow.getHaltMsg();
		if (haltMsg.isOk()) {
			for (String output : haltMsg.getResult()) {
				outputs.add(files.containsKey(output) ? files.get(output).getHdfsPath().toString() : output);
			}
		}
		return outputs;
	}

	private Collection<TaskInstance> getNextTasks() {
		Collection<TaskInstance> tasks = new LinkedList<>();

		while (workflow.hasNextRequest()) {
			JSONObject request = workflow.nextRequest();

			// TODO: parse request
			String taskName = RemoteWorkflow.getLamName(request);

			CuneiformETaskInstance task = new CuneiformETaskInstance(getRunId(), taskName);
			requests.put(task, request);
			for (String fileName : RemoteWorkflow.getInputSet(request)) {
				Data file = files.get(fileName);
				if (file == null) {
					file = new Data(fileName);
					files.put(fileName, file);
				}
				task.addInputData(file);
			}

			try (BufferedWriter writer = new BufferedWriter(new FileWriter(task.getId() + "_request"))) {
				writer.write(request.toString());
			} catch (IOException e) {
				e.printStackTrace(System.out);
				System.exit(-1);
			}

			task.setCommand("effi -r true " + task.getId() + "_request " + task.getId() + "_reply");
			tasks.add(task);

			writeEntryToLog(new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), Long.valueOf(task.getId()),
			    null, JsonReportEntry.KEY_INVOC_SCRIPT, task.getCommand()));
			writeEntryToLog(new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), Long.valueOf(task.getId()),
			    null, JsonReportEntry.KEY_INVOC_EXEC, request.toString()));
		}

		return tasks;
	}

	@Override
	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		try {
			(new Data(task.getId() + "_reply", containerId.toString())).stageIn();
			JSONObject reply = parseEffiFile(task.getId() + "_reply");
			workflow.addReply(reply);

			writeEntryToLog(new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), Long.valueOf(task.getId()),
			    null, JsonReportEntry.KEY_INVOC_OUTPUT, reply.toString()));
			for (String fileName : RemoteWorkflow.getOutputSet(requests.get(task), reply)) {
				files.put(fileName, new Data(fileName, containerId.toString()));
			}
		} catch (IOException | JSONException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
	}

	public static JSONObject parseEffiFile(String fileName) throws JSONException {
		StringBuilder sb = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return new JSONObject(sb.toString());
	}

}
