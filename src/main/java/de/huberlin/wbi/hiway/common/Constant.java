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

public class Constant {

	// the name of Hi-WAY applications, as shown in the RM web interface
	public static final String APPLICATION_TYPE = "WORKFLOW";

	// the directory in which Hi-WAY stores intermediate and output files
	public static final String SANDBOX_DIRECTORY = "hiway";

	// the names of scripts created by Hi-WAY for each invocation
	public static final String SUPER_SCRIPT_FILENAME = "__super_script__";
	public static final String PRE_SCRIPT_FILENAME = "__pre_script__";
	public static final String POST_SCRIPT_FILENAME = "__post_script__";
	public static final String BASH_SHEBANG = "#!/usr/bin/env bash\n";

	// Keys of entries in the log
	public static final String KEY_INVOC_HOST = "invoc-host";
	public static final String KEY_INVOC_TIME_STAGEIN = "invoc-time-stagein";
	public static final String KEY_INVOC_TIME_STAGEOUT = "invoc-time-stageout";
	public static final String KEY_FILE_TIME_STAGEIN = "file-time-stagein";
	public static final String KEY_FILE_TIME_STAGEOUT = "file-time-stageout";
	public static final String KEY_INVOC_TIME_SCHED = "invoc-time-sched";
	public static final String KEY_WF_NAME = "wf-name";
	public static final String KEY_WF_TIME = "wf-time";
	public static final String KEY_HIWAY_EVENT = "hiway-event";
	
	// the amount of memory (in MB) a single instance of the hdfs command requires
	public static final int HDFS_MEMORY_REQ = 768;
	
	// available scheduling policies
	public static enum SchedulingPolicy {
		staticRoundRobin, heft, greedyQueue, c3po, conservative, cloning, placementAware, outlooking
	}

	// available workflow formats
	public static enum WorkflowFormat {
		cuneiform, dax
	}

	// how often a task is to be retried before the workflow fails
	public static int retries = 1;

}
