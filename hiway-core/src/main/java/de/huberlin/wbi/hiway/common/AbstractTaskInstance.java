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
package de.huberlin.wbi.hiway.common;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;

public class AbstractTaskInstance implements Comparable<TaskInstance>,
		TaskInstance {

	public static class Comparators {

		public static Comparator<TaskInstance> DEPTH = new Comparator<TaskInstance>() {
			@Override
			public int compare(TaskInstance task1, TaskInstance task2) {
				try {
					return Integer.compare(task1.getDepth(), task2.getDepth());
				} catch (WorkflowStructureUnknownException e) {
					throw new RuntimeException(e);
				}
			};
		};

		public static Comparator<TaskInstance> UPWARDSRANK = new Comparator<TaskInstance>() {
			@Override
			public int compare(TaskInstance task1, TaskInstance task2) {
				try {
					return -Double.compare(task1.getUpwardRank(),
							task2.getUpwardRank());
				} catch (WorkflowStructureUnknownException e) {
					throw new RuntimeException(e);
				}
			};
		};

	}

	private static int runningId = 0;

	private Set<TaskInstance> childTasks;

	// the command to be executed
	private String command;
	// whether this task is completed yet
	private boolean completed;

	private int depth = 0;

	// this task instance's id
	private final long id;

	// input and output data
	private Set<Data> inputData;

	// the programming language of this task (default: bash)
	private String languageLabel;

	private Set<Data> outputData;

	// parent and child tasks (denotes the workflow structure)
	private Set<TaskInstance> parentTasks;
	private Set<JsonReportEntry> report;

	// the scripts associated with this task
	// private Data superScript;
	// private Set<Data> scripts;

	private long signature;

	// HEFT parameters
	// the depth of the task in the workflow (input tasks have depth 0)

	private long taskId;

	// the name and (internal) id of the task's executable (e.g. tar)
	private String taskName;

	// the number of times this task has been attempted
	private int tries = 0;
	// the upward rank of tasks in the workflow
	private double upwardRank = 0d;

	// the id of the workflow this task instance belongs to
	private UUID workflowId;

	public AbstractTaskInstance(UUID workflowId, String taskName, long taskId) {
		this(workflowId, taskName, taskId, ForeignLambdaExpr.LANGID_BASH);
	}

	public AbstractTaskInstance(UUID workflowId, String taskName, long taskId,
			String languageLabel) {
		this.id = runningId++;
		this.workflowId = workflowId;
		this.taskName = taskName;
		this.taskId = taskId;
		this.languageLabel = languageLabel;

		this.completed = false;
		// scripts = new CopyOnWriteArraySet<>();
		this.inputData = new HashSet<>();
		this.outputData = new HashSet<>();
		this.report = new HashSet<>();
		this.parentTasks = new HashSet<>();
		this.childTasks = new HashSet<>();
	}

	@Override
	public void addChildTask(TaskInstance childTask)
			throws WorkflowStructureUnknownException {
		childTasks.add(childTask);
	}

	@Override
	public void addInputData(Data data) {
		inputData.add(data);
	}

	@Override
	public void addOutputData(Data data) {
		outputData.add(data);
	}

	@Override
	public void addParentTask(TaskInstance parentTask)
			throws WorkflowStructureUnknownException {
		parentTasks.add(parentTask);
		this.setDepth(parentTask.getDepth() + 1);
	}

	// @Override
	// public void addScript(Data script) {
	// scripts.add(script);
	// }

	@Override
	public int compareTo(TaskInstance other) {
		return Long.compare(this.getId(), other.getId());
	}

	@Override
	public long countAvailableLocalData(FileSystem fs, Container container)
			throws IOException {
		long sum = 0;
		for (Data input : inputData) {
			sum += input.countAvailableLocalData(fs, container);
		}
		return sum;
	}

	@Override
	public long countAvailableTotalData(FileSystem fs) throws IOException {
		long sum = 0;
		for (Data input : inputData) {
			sum += input.countAvailableTotalData(fs);
		}
		return sum;
	}

	@Override
	public Set<TaskInstance> getChildTasks()
			throws WorkflowStructureUnknownException {
		return childTasks;
	}

	@Override
	public String getCommand() {
		return command;
	}

	@Override
	public int getDepth() throws WorkflowStructureUnknownException {
		return depth;
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public Set<Data> getInputData() {
		return inputData;
	}

	@Override
	public String getLanguageLabel() {
		return languageLabel;
	}

	@Override
	public Set<Data> getOutputData() {
		return outputData;
	}

	@Override
	public Set<TaskInstance> getParentTasks()
			throws WorkflowStructureUnknownException {
		return parentTasks;
	}

	@Override
	public Set<JsonReportEntry> getReport() {
		return report;
	}

	// @Override
	// public Set<Data> getScripts() {
	// return scripts;
	// }

	@Override
	public long getSignature() {
		return signature;
	}

	@Override
	public long getTaskId() {
		return taskId;
	}

	@Override
	public String getTaskName() {
		return taskName;
	}

	@Override
	public int getTries() {
		return tries;
	}

	@Override
	public double getUpwardRank() throws WorkflowStructureUnknownException {
		return upwardRank;
	}

	@Override
	public UUID getWorkflowId() {
		return workflowId;
	}

	@Override
	public void incTries() {
		tries++;
	}

	@Override
	public boolean isCompleted() {
		return completed;
	}

	@Override
	public boolean readyToExecute() {
		for (TaskInstance parentTask : parentTasks) {
			if (!parentTask.isCompleted())
				return false;
		}
		return true;
	}

	@Override
	public boolean retry() {
		return tries <= Constant.retries;
	}

	@Override
	public void setCommand(String command) {
		this.command = command;
	}

	@Override
	public void setCompleted() {
		completed = true;
	}

	@Override
	public void setDepth(int depth) throws WorkflowStructureUnknownException {
		if (this.depth < depth) {
			this.depth = depth;
			for (TaskInstance child : this.getChildTasks()) {
				child.setDepth(depth + 1);
			}
		}
	}

	@Override
	public void setSignature(long signature) {
		this.signature = signature;
	}

	@Override
	public void setUpwardRank(double upwardRank)
			throws WorkflowStructureUnknownException {
		this.upwardRank = upwardRank;
	}

	@Override
	public String toString() {
		return id + " [" + taskName + "]";
	}

}
