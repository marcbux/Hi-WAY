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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;

import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public class TaskInstance implements Comparable<TaskInstance> {

	public static class Comparators {

		public static Comparator<TaskInstance> DEPTH = new Comparator<TaskInstance>() {
			@Override
			public int compare(TaskInstance task1, TaskInstance task2) {
				try {
					return Integer.compare(task1.getDepth(), task2.getDepth());
				} catch (WorkflowStructureUnknownException e) {
					e.printStackTrace();
					System.exit(1);
					throw new RuntimeException(e);
				}
			};
		};

		public static Comparator<TaskInstance> UPWARDSRANK = new Comparator<TaskInstance>() {
			@Override
			public int compare(TaskInstance task1, TaskInstance task2) {
				try {
					return -Double.compare(task1.getUpwardRank(), task2.getUpwardRank());
				} catch (WorkflowStructureUnknownException e) {
					e.printStackTrace();
					System.exit(1);
					throw new RuntimeException(e);
				}
			};
		};

	}

	private static int runningId = 1;
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
	private long taskId;
	// the name and (internal) id of the task's executable (e.g. tar)
	private String taskName;
	// the number of times this task has been attempted
	private int tries = 0;
	// the upward rank of tasks in the workflow
	private double upwardRank = 0d;
	// the id of the workflow this task instance belongs to
	private UUID workflowId;

	public TaskInstance(UUID workflowId, String taskName, long taskId) {
		this(workflowId, taskName, taskId, ForeignLambdaExpr.LANGID_BASH);
	}

	public TaskInstance(UUID workflowId, String taskName, long taskId, String languageLabel) {
		this(runningId++, workflowId, taskName, taskId, ForeignLambdaExpr.LANGID_BASH);
	}
	
	public TaskInstance(long id, UUID workflowId, String taskName, long taskId, String languageLabel) {
		this.id = id;
		this.workflowId = workflowId;
		this.taskName = taskName;
		this.taskId = taskId;
		this.languageLabel = languageLabel;

		this.completed = false;
		this.inputData = new HashSet<>();
		this.outputData = new HashSet<>();
		this.report = new HashSet<>();
		this.parentTasks = new HashSet<>();
		this.childTasks = new HashSet<>();
	}

	public void addChildTask(TaskInstance childTask) throws WorkflowStructureUnknownException {
		childTasks.add(childTask);
	}

	public void addInputData(Data data) {
		inputData.add(data);
	}

	public void addOutputData(Data data) {
		outputData.add(data);
	}

	public void addParentTask(TaskInstance parentTask) throws WorkflowStructureUnknownException {
		parentTasks.add(parentTask);
		this.setDepth(parentTask.getDepth() + 1);
	}

	public int compareTo(TaskInstance other) {
		return Long.compare(this.getId(), other.getId());
	}

	public long countAvailableLocalData(FileSystem fs, Container container) throws IOException {
		long sum = 0;
		for (Data input : getInputData()) {
			sum += input.countAvailableLocalData(fs, container);
		}
		return sum;
	}

	public long countAvailableTotalData(FileSystem fs) throws IOException {
		long sum = 0;
		for (Data input : getInputData()) {
			sum += input.countAvailableTotalData(fs);
		}
		return sum;
	}

	public Set<TaskInstance> getChildTasks() throws WorkflowStructureUnknownException {
		return childTasks;
	}

	public String getCommand() {
		return command;
	}

	public int getDepth() throws WorkflowStructureUnknownException {
		return depth;
	}

	public long getId() {
		return id;
	}

	public Set<Data> getInputData() {
		return inputData;
	}

	public String getLanguageLabel() {
		return languageLabel;
	}

	public Set<Data> getOutputData() {
		return outputData;
	}

	public Set<TaskInstance> getParentTasks() throws WorkflowStructureUnknownException {
		return parentTasks;
	}

	public Set<JsonReportEntry> getReport() {
		return report;
	}

	public long getTaskId() {
		return taskId;
	}

	public String getTaskName() {
		return taskName;
	}

	public int getTries() {
		return tries;
	}

	public double getUpwardRank() throws WorkflowStructureUnknownException {
		return upwardRank;
	}

	public UUID getWorkflowId() {
		return workflowId;
	}

	public void incTries() {
		tries++;
	}

	public boolean isCompleted() {
		return completed;
	}

	public boolean readyToExecute() {
		for (TaskInstance parentTask : parentTasks) {
			if (!parentTask.isCompleted())
				return false;
		}
		return true;
	}

	public boolean retry(int maxRetries) {
		return tries <= maxRetries;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public void setCompleted() {
		completed = true;
	}

	public void setDepth(int depth) throws WorkflowStructureUnknownException {
		if (this.depth < depth) {
			this.depth = depth;
			for (TaskInstance child : this.getChildTasks()) {
				child.setDepth(depth + 1);
			}
		}
	}

	public void setUpwardRank(double upwardRank) throws WorkflowStructureUnknownException {
		this.upwardRank = upwardRank;
	}

	public String toString() {
		return id + " [" + taskName + "]";
	}

}
