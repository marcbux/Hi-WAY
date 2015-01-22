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
package de.huberlin.wbi.hiway.common;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.source.JvmMetrics;

@Metrics(about = "Workflow Application Master Metrics", context = "hiway")
public class WFAppMetrics {

	public static WFAppMetrics create() {
		return create(DefaultMetricsSystem.instance());
	}

	public static WFAppMetrics create(MetricsSystem ms) {
		JvmMetrics.initSingleton("WFAppMaster", null);
		return ms.register(new WFAppMetrics());
	}

	@Metric
	private MutableCounterInt tasksCompleted;
	@Metric
	private MutableCounterInt tasksFailed;
	@Metric
	private MutableCounterInt tasksKilled;
	@Metric
	private MutableCounterInt tasksLaunched;
	@Metric
	private MutableGaugeInt tasksRunning;
	@Metric
	private MutableGaugeInt tasksWaiting;

	public void completedTask(TaskInstance task) {
		tasksCompleted.incr();
	}

	public void endRunningTask(TaskInstance task) {
		tasksRunning.decr();
	}

	public void endWaitingTask() {
		tasksWaiting.decr();
	}

	public void failedTask(TaskInstance task) {
		tasksFailed.incr();
	}

	public void killedTask(TaskInstance task) {
		tasksKilled.incr();
	}

	public void launchedTask(TaskInstance task) {
		tasksLaunched.incr();
	}

	public void runningTask(TaskInstance task) {
		tasksRunning.incr();
	}

	public void waitingTask() {
		tasksWaiting.incr();
	}

}
