package de.huberlin.wbi.hiway.am.cuneiforme;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;

import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;
import de.huberlin.wbi.hiway.common.HiWayConfiguration.HIWAY_WORKFLOW_LANGUAGE_OPTS;

public class CuneiformETaskInstance extends TaskInstance {

	public CuneiformETaskInstance(UUID workflowId, String taskName) {
		super(workflowId, taskName, Math.abs(taskName.hashCode() + 1));
	}

	@Override
	public Map<String, LocalResource> buildScriptsAndSetResources(Container container) {
		Map<String, LocalResource> localResources = super.buildScriptsAndSetResources(container);

		Data preSriptData = new Data(id + "_request", container.getId().toString());
		try {
			preSriptData.stageOut();
			preSriptData.addToLocalResourceMap(localResources);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return localResources;
	}
	
	@Override
	public void addChildTask(TaskInstance childTask) throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}

	@Override
	public void addParentTask(TaskInstance parentTask) throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}

	@Override
	public Set<TaskInstance> getChildTasks() throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}

	@Override
	public int getDepth() throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}
	
	@Override
	public Set<TaskInstance> getParentTasks() throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}

	@Override
	public double getUpwardRank() throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}
	
	@Override
	public void setDepth(int depth) throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}

	@Override
	public void setUpwardRank(double upwardRank) throws WorkflowStructureUnknownException {
		throw new WorkflowStructureUnknownException(HIWAY_WORKFLOW_LANGUAGE_OPTS.cuneiformE);
	}

}
