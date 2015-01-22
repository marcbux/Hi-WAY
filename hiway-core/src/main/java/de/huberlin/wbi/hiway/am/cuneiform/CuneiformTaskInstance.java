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
package de.huberlin.wbi.hiway.am.cuneiform;

import java.util.Set;

import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

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
