package de.huberlin.wbi.hiway.am.cuneiforme;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;

import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;

public class CuneiformETaskInstance extends TaskInstance {

	public CuneiformETaskInstance(UUID workflowId, String taskName, long taskId) {
		super(workflowId, taskName, taskId);
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

}
