/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * J�rgen Brandt (HU Berlin)
 * Marc Bux (HU Berlin)
 * Ulf Leser (HU Berlin)
 *
 * J�rgen Brandt is funded by the European Commission through the BiobankCloud
 * project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 * research training group SOAMED (GRK 1651).
 *
 * Copyright 2014 Humboldt-Universit�t zu Berlin
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
package de.huberlin.wbi.hiway.common;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public interface TaskInstance {

	public void addChildTask(TaskInstance childTask)
			throws WorkflowStructureUnknownException;

	public void addInputData(Data data);

	public void addOutputData(Data data);

	public void addParentTask(TaskInstance parentTask)
			throws WorkflowStructureUnknownException;

//	public void addScript(Data script);

	public long countAvailableLocalData(FileSystem fs, Container container)
			throws IOException;

	public long countAvailableTotalData(FileSystem fs) throws IOException;

	public Set<TaskInstance> getChildTasks()
			throws WorkflowStructureUnknownException;

	public String getCommand();

	public int getDepth() throws WorkflowStructureUnknownException;

	public int getId();

	public Set<Data> getInputData();

	public String getLanguageLabel();

	public Set<Data> getOutputData();

	public Set<TaskInstance> getParentTasks()
			throws WorkflowStructureUnknownException;

	public Set<JsonReportEntry> getReport();

//	public Set<Data> getScripts();

	public long getSignature();

	public long getTaskId();

	public String getTaskName();

	public int getTries();

	public double getUpwardRank() throws WorkflowStructureUnknownException;

	public UUID getWorkflowId();

	public void incTries();

	public boolean isCompleted();

	public boolean readyToExecute();

	public boolean retry();

	public void setCommand(String command);

	public void setCompleted();

	public void setDepth(int depth) throws WorkflowStructureUnknownException;

	public void setSignature(long signature);

	public void setUpwardRank(double upwardRank)
			throws WorkflowStructureUnknownException;

}
