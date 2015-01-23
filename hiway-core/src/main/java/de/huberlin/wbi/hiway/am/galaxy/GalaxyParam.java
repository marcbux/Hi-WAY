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
package de.huberlin.wbi.hiway.am.galaxy;

import java.util.Set;

/**
 * An abstract class for the parameters of a Galaxy tool. A parameter can either be atomic, or it can be composite in the form of a repeat or conditional
 * parameter.
 * 
 * @author Marc Bux
 *
 */
public abstract class GalaxyParam {
	protected final String name;

	public GalaxyParam(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	/**
	 * This function recursively iterates over the children of this parameter and gathers all descendants
	 * 
	 * @return the set of atomic parameters that are descendants of this parameter (including the parameter itself, if it is atomic)
	 */
	public abstract Set<GalaxyParamValue> getParamValues();
}
