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
package de.huberlin.wbi.hiway.scheduler;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.math3.distribution.NormalDistribution;

public class WienerProcessModel {
	protected long botId;
	protected String hostId;

	protected boolean logarithmize;

	protected Deque<Runtime> measurements;
	protected Queue<Double> differences;
	protected double sumOfDifferences;
	protected Deque<Runtime> estimates;

	public WienerProcessModel(long botId, String hostId, boolean logarithmize) {
		this.botId = botId;
		this.hostId = hostId;

		this.logarithmize = logarithmize;

		measurements = new LinkedList<>();
		differences = new LinkedList<>();
		estimates = new LinkedList<>();
	}

	public void addRuntime(double timestamp, double runtime) {
		Runtime measurement = new Runtime(timestamp, logarithmize ? Math.log(runtime) : runtime);
		if (!measurements.isEmpty()) {
			Runtime lastMeasurement = measurements.getLast();
			double difference = (measurement.runtime - lastMeasurement.runtime) / Math.sqrt(measurement.timestamp - lastMeasurement.timestamp);
			sumOfDifferences += difference;
			differences.add(difference);
		}
		measurements.add(measurement);
	}

	public double getEstimate(double timestamp, double alpha) {
		if (alpha == 0.5 && measurements.size() > 0) {
			return logarithmize ? Math.pow(Math.E, measurements.getLast().runtime) : Math.max(measurements.getLast().runtime, Double.MIN_NORMAL);
		}
		
		if (differences.size() < 2) {
			return 0d;
		}

		Runtime lastMeasurement = measurements.getLast();

		double variance = 0d;
		double avgDifference = sumOfDifferences / differences.size();
		for (double difference : differences) {
			variance += Math.pow(difference - avgDifference, 2d);
		}
		variance /= differences.size() - 1;

		variance *= timestamp - lastMeasurement.timestamp;

		double estimate = lastMeasurement.runtime;
		if (variance > 0d) {
			NormalDistribution nd = new NormalDistribution(lastMeasurement.runtime, Math.sqrt(variance));
			estimate = nd.inverseCumulativeProbability(alpha);
		}

		estimate = logarithmize ? Math.pow(Math.E, estimate) : Math.max(estimate, 0d);

		return estimate;
	}

	public double getEstimate(double alpha) {
		return getEstimate(System.currentTimeMillis(), alpha);
	}
	
	public double getEstimate() {
		return getEstimate(0.5);
	}
}
