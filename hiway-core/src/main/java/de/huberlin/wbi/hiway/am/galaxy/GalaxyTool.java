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

import org.json.JSONException;
import org.json.JSONObject;

/**
 * An object that provides information an tools registered in a Galaxy installation.
 * 
 * @author Marc Bux
 *
 */
public class GalaxyTool {
	// the environment that has to be set prior to running this tool
	private String environment;
	// this tool's id, as provided in its XML description file
	private final String id;
	// these tool's parameters
	private Set<GalaxyParam> params;
	// the packages (name, version) that are required to be installed for this tool to run; these requirements are parsed to determine the tool's environment
	private Map<String, String> requirements;
	// the template for the command to run this tool; the template will have to be compiled by Cheetah at runtime to resolve parameters
	private String template;
	// this tool's version number, as provided in its XML description file
	private final String version;

	public GalaxyTool(String id, String version, String dir) {
		this.id = id;
		this.version = version;
		params = new HashSet<>();
		this.environment = "PATH=" + dir + ":$PATH; export PATH\n";
		requirements = new HashMap<>();
	}

	/**
	 * A method for adding commands to be executed before the tool is invoked, e.g., to set the environment in which the toolis to be run
	 * 
	 * @param env
	 *            the shell command to be executed before the tool is invoked
	 */
	public void addEnv(String env) {
		environment = environment + (env.endsWith("\n") ? env : env + "\n");
	}

	public void addParam(GalaxyParam param) {
		params.add(param);
	}

	public void addRequirement(String reqname, String reqversion) {
		requirements.put(reqname, reqversion);
	}

	public String getEnv() {
		return environment;
	}

	/**
	 * A function that recursively iterates through the parameters of this tool and returns the first parameter matching the specified name.
	 * 
	 * @param name
	 *            the name of the parameter to be retrieved
	 * @return the first atomic parameter matching the specified name
	 */
	public GalaxyParamValue getFirstMatchingParamByName(String name) {
		for (GalaxyParam param : params)
			for (GalaxyParamValue paramValue : param.getParamValues())
				if (paramValue.getName().equals(name))
					return paramValue;
		return null;
	}

	public String getId() {
		return id;
	}

	public Set<String> getRequirements() {
		return requirements.keySet();
	}

	public String getRequirementVersion(String name) {
		return requirements.get(name);
	}

	public String getTemplate() {
		return template;
	}

	public String getUniqueId() {
		return getId() + "/" + getVersion();
	}

	public String getVersion() {
		return version;
	}

	@Override
	public int hashCode() {
		return getUniqueId().hashCode();
	}

	/**
	 * A function that maps the values of a given JSON object (e.g. an invocation's tool state) according to the mappings defined in this Galaxy Tool
	 * 
	 * @param jo
	 *            the JSON object whose values are to be mapped
	 * @throws JSONException
	 */
	public void mapParams(JSONObject jo) throws JSONException {
		if (jo.length() == 0)
			return;
		for (String name : JSONObject.getNames(jo)) {
			Object value = jo.get(name);
			if (value instanceof JSONObject) {
				mapParams((JSONObject) value);
			} else {
				GalaxyParamValue paramValue = getFirstMatchingParamByName(name);
				if (paramValue != null && paramValue.hasMapping(value)) {
					jo.put(name, paramValue.getMapping(value));
				} else if (value.equals(JSONObject.NULL)) {
					jo.remove(name);
				}
			}
		}
	}

	public void setParams(Set<GalaxyParam> params) {
		this.params = params;
	}

	/**
	 * A function that appends the string ".path" to all occurrences of a file parameter name in the template; this is done since a file parameter has a whole
	 * JSON object / Python dictionary of attributes (e.g., its path, its metadata, its extension) and therefore can't be accessed directly by its name
	 * 
	 * @param name
	 *            the name of a file parameter
	 */
	public void setPath(String fileName) {
		template = template.replaceAll("(\\$[^\\s]*)" + fileName + "([\\}'\"\\s]+)($|[^i]|i[^n]|in[^\\s])", "$1" + fileName + ".path$2$3");
	}

	public void setTemplate(String template) {
		this.template = template.endsWith("\n") ? template : template + "\n";
	}

	@Override
	public String toString() {
		return getId();
	}
}
