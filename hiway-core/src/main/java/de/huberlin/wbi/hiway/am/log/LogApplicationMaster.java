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
package de.huberlin.wbi.hiway.am.log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.am.HiWay;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class LogApplicationMaster extends HiWay {

	public static void main(String[] args) {
		HiWay.loop(new LogApplicationMaster(), args);
	}

	@Override
	public void parseWorkflow() {
		System.out.println("Parsing Hi-WAY log " + getWorkflowFile());
		Map<Long, TaskInstance> tasks = new HashMap<>();
		Map<Data, TaskInstance> taskProcucingThisFile = new HashMap<>();

		try (BufferedReader reader = new BufferedReader(new FileReader(getWorkflowFile().getLocalPath().toString()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				try {
					JsonReportEntry entry = new JsonReportEntry(line);
					Long invocId = entry.getInvocId();
					if (invocId != null && !tasks.containsKey(invocId)) {
						tasks.put(invocId, new TaskInstance(invocId, getRunId(), entry.getTaskName(), entry.getTaskId().longValue(), entry.getLang()));
					}
					TaskInstance task = tasks.get(invocId);

					switch (entry.getKey()) {
					case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
					case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
					case JsonReportEntry.KEY_INVOC_SCRIPT:
					case JsonReportEntry.KEY_INVOC_EXEC:
					case JsonReportEntry.KEY_INVOC_USER:
					case JsonReportEntry.KEY_INVOC_OUTPUT:
						updateAndRegisterEntry(entry, task);
						break;
					default:
					}

					switch (entry.getKey()) {
					case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
						String inputName = entry.getFile();
						if (!getFiles().containsKey(inputName)) {
							Data data = new Data(inputName, getFs());
							data.setInput(true);
							getFiles().put(inputName, data);
						}
						Data data = getFiles().get(inputName);
						task.addInputData(data);
						break;
					case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
						String outputName = entry.getFile();
						if (!getFiles().containsKey(outputName)) {
							data = new Data(outputName, getFs());
							getFiles().put(outputName, data);
						}
						data = getFiles().get(outputName);
						task.addOutputData(data);
						data.setInput(false);
						taskProcucingThisFile.put(data, task);
						break;
					case JsonReportEntry.KEY_INVOC_SCRIPT:
						task.setCommand(entry.getValueRawString());
						break;
					case HiwayDBI.KEY_WF_OUTPUT:
						String[] outputs = entry.getValueRawString().split(", ");
						for (String output : outputs) {
							getFiles().get(output).setOutput(true);
						}
						break;
					default:
					}
				} catch (JSONException e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		for (TaskInstance task : tasks.values()) {
			for (Data data : task.getInputData()) {
				if (data.isInput())
					continue;
				TaskInstance parentTask = taskProcucingThisFile.get(data);
				try {
					task.addParentTask(parentTask);
					parentTask.addChildTask(task);
				} catch (WorkflowStructureUnknownException e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
		}

		getScheduler().addTasks(tasks.values());
	}

	private void updateAndRegisterEntry(JsonReportEntry entry, TaskInstance task) {
		entry.setRunId(getRunId());
		entry.setInvocId(task.getId());
		writeEntryToLog(entry);
	}

}
