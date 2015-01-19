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
package de.huberlin.wbi.hiway.app.am;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class LogApplicationMaster extends HiWay {
	
	private static final Log log = LogFactory.getLog(LogApplicationMaster.class);

	public static void main(String[] args) {
		HiWay.loop(new LogApplicationMaster(), args);
	}
	
	@Override
	public void parseWorkflow() {
		log.info("Parsing Hi-WAY log " + workflowFile);
		Map<Long, TaskInstance> tasks = new HashMap<>();
		Map<Data, TaskInstance> taskProcucingThisFile = new HashMap<>();
		
		try (BufferedReader reader = new BufferedReader(new FileReader(workflowFile.getLocalPath()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				try {
					JsonReportEntry entry = new JsonReportEntry(line);
					Long invocId = entry.getInvocId();
					if (invocId != null && !tasks.containsKey(invocId)) {
						tasks.put(invocId, new TaskInstance(getRunId(), entry.getTaskName(), entry.getTaskId(), entry.getLang()));
					}
					TaskInstance task = tasks.get(invocId);
					
					switch (entry.getKey()) {
					case JsonReportEntry.KEY_FILE_SIZE_STAGEIN:
						String inputName = entry.getFile();
						if (!files.containsKey(inputName)) {
							Data data = new Data(inputName);
							data.setInput(true);
							files.put(inputName, data);
						}
						Data data = files.get(inputName);
						task.addInputData(data);
						data.setOutput(false);
						break;
					case JsonReportEntry.KEY_FILE_SIZE_STAGEOUT:
						String outputName = entry.getFile();
						if (!files.containsKey(outputName)) {
							data = new Data(outputName);
							data.setOutput(true);
							files.put(outputName, data);
						}
						data = files.get(outputName);
						task.addOutputData(data);
						data.setInput(false);
						taskProcucingThisFile.put(data, task);
						break;
					case JsonReportEntry.KEY_INVOC_SCRIPT:
						task.setCommand(entry.getValueRawString());
					case JsonReportEntry.KEY_INVOC_OUTPUT:
					case JsonReportEntry.KEY_INVOC_EXEC:
					case JsonReportEntry.KEY_INVOC_USER:
						entry.setRunId(getRunId());
						entry.setInvocId(task.getId());
						task.getReport().add(entry);
						break;
					}
				} catch (JSONException e) {
					onError(e);
				}
			}
		} catch (IOException e) {
			HiWay.onError(e);
		}
		
		for (TaskInstance task : tasks.values()) {
			for (Data data : task.getInputData()) {
				if (data.isInput()) continue;
				TaskInstance parentTask = taskProcucingThisFile.get(data);
				try {
					task.addParentTask(parentTask);
					parentTask.addChildTask(task);
				} catch (WorkflowStructureUnknownException e) {
					onError(e);
				}
			}
		}
		
		scheduler.addTasks(tasks.values());
	}

}
