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

/**
 * An atomic parameter of a Galaxy tool.
 * 
 * @author Marc Bux
 *
 */
public class GalaxyParamValue extends GalaxyParam {
	// this parameter's data type extension, if any
	private String dataType;
	// some parameters describe files produced in a working dir, which will have to be moved after computation; this parameter stores where to obtain the data
	private String from_work_dir;
	// some parameters are set to a certain value (e.g. "true", "false"), yet in order to invoke the Galaxy tool, they have to be mapped to a different notation
	// (e.g. "1", "0"); this map stores all the mappings for values of this parameter.
	private Map<Object, Object> mappings;

	public GalaxyParamValue(String name) {
		super(name);
		mappings = new HashMap<>();
	}

	public void addMapping(Object from, Object to) {
		mappings.put(from, to);
	}

	public String getDataType() {
		return dataType;
	}

	public String getFrom_work_dir() {
		return from_work_dir;
	}

	public Object getMapping(Object from) {
		return mappings.get(from);
	}

	@Override
	public Set<GalaxyParamValue> getParamValues() {
		Set<GalaxyParamValue> paramValues = new HashSet<>();
		paramValues.add(this);
		return paramValues;
	}

	public boolean hasFrom_work_dir() {
		return from_work_dir != null && from_work_dir.length() > 0;
	}

	public boolean hasMapping(Object from) {
		Object to = mappings.get(from);
		return to != null;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public void setFrom_work_dir(String from_work_dir) {
		this.from_work_dir = from_work_dir;
	}
}
