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
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.json.JSONException;

import de.huberlin.wbi.cfjava.cuneiform.Workflow;
import de.huberlin.wbi.hiway.am.HiWay;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class CuneiformEApplicationMaster extends HiWay {

	Workflow workflow;

	public static void main(String[] args) {
		HiWay.loop(new CuneiformEApplicationMaster(), args);
	}

	public CuneiformEApplicationMaster() {
		super();
		setDetermineFileSizes();
	}

	@Override
	public Collection<TaskInstance> parseWorkflow() {
		System.out.println("Parsing Cuneiform workflow " + getWorkflowFile());

		try (BufferedReader reader = new BufferedReader(new FileReader(getWorkflowFile().getLocalPath().toString()))) {
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
			workflow = new Workflow(sb.toString());
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		done = workflow.reduce();

		// obtain requests, create task instances from them, and return them
		return new LinkedList<>();
	}

	@Override
	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		// TODO Auto-generated method stub
		super.taskSuccess(task, containerId);
	}

	@Override
	public void taskFailure(TaskInstance task, ContainerId containerId) {
		// TODO Auto-generated method stub
		super.taskFailure(task, containerId);
	}

}
