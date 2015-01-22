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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GalaxyConditional extends GalaxyParam {
	GalaxyParamValue condition;
	Map<String, Set<GalaxyParam>> conditionalParams;

	public GalaxyConditional(String name) {
		super(name);
		conditionalParams = new HashMap<>();
	}

	@Override
	public Set<GalaxyParamValue> getParamValues() {
		Set<GalaxyParamValue> paramValues = new HashSet<>();
		paramValues.add(condition);
		for (Set<GalaxyParam> params : conditionalParams.values())
			for (GalaxyParam param : params)
				paramValues.addAll(param.getParamValues());
		return paramValues;
	}

	public void setCondition(GalaxyParamValue condition) {
		this.condition = condition;
	}

	public void setConditionalParams(String conditionValue, Set<GalaxyParam> params) {
		conditionalParams.put(conditionValue, params);
	}
}
