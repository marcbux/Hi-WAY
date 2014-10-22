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
package de.huberlin.wbi.hiway.app;

import org.apache.hadoop.conf.Configuration;

public class HiWayConfiguration extends Configuration {

	public static enum HIWAY_DB_TYPE_OPTS {
		local, NoSQL, SQL
	}

	public static enum HIWAY_SCHEDULER_OPTS {
		c3po, cloning, conservative, greedyQueue, heft, outlooking, placementAware, staticRoundRobin
	}

	public static enum HIWAY_WORKFLOW_LANGUAGE_OPTS {
		cuneiform, dax, log, galaxy
	}

	public static final String HIWAY_AM_APPLICATION_TYPE = "hiway.am.application.type";
	public static final String HIWAY_AM_APPLICATION_TYPE_DEFAULT = "Hi-WAY";
	public static final String HIWAY_AM_MEMORY = "hiway.am.memory";
	public static final int HIWAY_AM_MEMORY_DEFAULT = 1024;
	public static final String HIWAY_AM_PRIORITY = "hiway.am.priority";
	public static final int HIWAY_AM_PRIORITY_DEFAULT = 0;
	public static final String HIWAY_AM_QUEUE = "hiway.am.queue";
	public static final String HIWAY_AM_QUEUE_DEFAULT = "default";

	public static final String HIWAY_AM_SANDBOX_DIRECTORY = "hiway.am.sandbox.directory";
	public static final String HIWAY_AM_SANDBOX_DIRECTORY_DEFAULT = "hiway";
	
	public static final String HIWAY_AM_STATLOG = "hiway.am.statlog";
	public static final String HIWAY_AM_STATLOG_DEFAULT = "stat.log";

	public static final String HIWAY_AM_TASK_RETRIES = "hiway.am.task.retries";
	public static final int HIWAY_AM_TASK_RETRIES_DEFAULT = 1;

	public static final String HIWAY_AM_VCORES = "hiway.am.vcores";
	public static final int HIWAY_AM_VCORES_DEFAULT = 1;
	public static final String HIWAY_CLIENT_TIMEOUT = "hiway.am.timeout";
	public static final int HIWAY_CLIENT_TIMEOUT_DEFAULT = 86400;

	public static final String HIWAY_DB_NOSQL_BUCKET = "hiway.db.nosql.bucket";
	public static final String HIWAY_DB_NOSQL_PASSWORD = "hiway.db.nosql.password";
	public static final String HIWAY_DB_NOSQL_URLS = "hiway.db.nosql.urls";
	public static final String HIWAY_DB_SQL_PASSWORD = "hiway.db.sql.password";
	public static final String HIWAY_DB_SQL_URL = "hiway.db.sql.url";
	public static final String HIWAY_DB_SQL_USER = "hiway.db.sql.user";
	public static final String HIWAY_DB_STAT_LOG = "hiway.stat.log";
	public static final String HIWAY_DB_STAT_LOG_DEFAULT = "stat.log";
	// allowed types: local, SQL, NoSQL
	public static final String HIWAY_DB_TYPE = "hiway.db.type";
	public static final HIWAY_DB_TYPE_OPTS HIWAY_DB_TYPE_DEFAULT = HIWAY_DB_TYPE_OPTS.local;
	public static final String HIWAY_SCHEDULER = "hiway.scheduler";
	public static final HIWAY_SCHEDULER_OPTS HIWAY_SCHEDULER_DEFAULT = HIWAY_SCHEDULER_OPTS.c3po;
	public static final String HIWAY_SITE_XML = "hiway-site.xml";
	public static final String HIWAY_WORKER_CLASS = "de.huberlin.wbi.hiway.app.Worker";

	public static final String HIWAY_WORKER_MEMORY = "hiway.worker.memory";
	public static final int HIWAY_WORKER_MEMORY_DEFAULT = 1024;
	public static final String HIWAY_WORKER_PRIORITY = "hiway.worker.priority";
	public static final int HIWAY_WORKER_PRIORITY_DEFAULT = 0;
	// Environment for shell script. Specified as env_key=env_val pairs
	public static final String HIWAY_WORKER_SHELL_ENV = "hiway.worker.shell.env";
	public static final String HIWAY_WORKER_SHELL_ENV_DEFAULT = "";
	public static final String HIWAY_WORKER_VCORES = "hiway.worker.vcores";
	public static final int HIWAY_WORKER_VCORES_DEFAULT = 1;
	
	public static final String HIWAY_WORKFLOW_LANGUAGE_CUNEIFORM_CLASS = "de.huberlin.wbi.hiway.app.am.CuneiformApplicationMaster";
	public static final String HIWAY_WORKFLOW_LANGUAGE_DAX_CLASS = "de.huberlin.wbi.hiway.app.am.DaxApplicationMaster";
	public static final String HIWAY_WORKFLOW_LANGUAGE_LOG_CLASS = "de.huberlin.wbi.hiway.app.am.LogApplicationMaster";
	public static final String HIWAY_WORKFLOW_LANGUAGE_GALAXY_CLASS = "de.huberlin.wbi.hiway.app.am.GalaxyApplicationMaster";

	static {
		addDefaultResource(HIWAY_SITE_XML);
	}

	public HiWayConfiguration() {
		super();
	}

	public HiWayConfiguration(Configuration conf) {
		super(conf);
	}

}
