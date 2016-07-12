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
package de.huberlin.wbi.hiway.am.dax;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;

public class DaxTaskInstance extends TaskInstance {

	private Map<Data, Long> fileSizes;
	private double runtime;

	public DaxTaskInstance(UUID workflowId, String taskName) {
		super(workflowId, taskName, Math.abs(taskName.hashCode() + 1));
		fileSizes = new HashMap<>();
	}

	public void addInputData(Data data, Long fileSize) {
		super.addInputData(data);
		fileSizes.put(data, fileSize);
	}

	public void addOutputData(Data data, Long fileSize) {
		super.addOutputData(data);
		fileSizes.put(data, fileSize);
	}

	@Override
	public String getCommand() {
		if (runtime > 0) {
			StringBuilder sb = new StringBuilder("sleep " + runtime + "\n");
			for (Data output : getOutputData()) {
				sb.append("dd if=/dev/zero of=" + output.getLocalPath() + " bs=" + fileSizes.get(output) + " count=1\n");
			}
			return sb.toString();
		}
		return super.getCommand();
	}

	@Override
	public Set<Data> getInputData() {
		if (runtime > 0) {
			Set<Data> intermediateData = new HashSet<>();
			for (Data input : super.getInputData()) {
				if (input.getContainerId() != null) {
					intermediateData.add(input);
				}
			}
			return intermediateData;
		}
		return super.getInputData();
	}

	public void setRuntime(double runtime) {
		this.runtime = runtime;
	}
}
