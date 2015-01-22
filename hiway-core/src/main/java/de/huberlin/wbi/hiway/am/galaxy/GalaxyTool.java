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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class GalaxyTool {
	private String env;
	private final String id;
	private Set<GalaxyParam> params;
	private Map<String, String> requirements;
	private String template;
	private final String version;

	public GalaxyTool(String id, String version, String dir, String galaxyPath) {
		this.id = id;
		this.version = version;
		params = new HashSet<>();
		this.env = "PATH=" + dir + ":$PATH; export PATH\n" + "PYTHONPATH=" + galaxyPath + "/lib:$PYTHONPATH; export PYTHONPATH\n";
		requirements = new HashMap<>();
	}

	public void addEnv(String env) {
		this.env = this.env + (env.endsWith("\n") ? env : env + "\n");
	}

	public void addFile(String name, GalaxyData data, JSONObject jo) {
		try {
			Pattern p = Pattern.compile("(_[0-9]*)?\\|");
			Matcher m = p.matcher(name);
			if (m.find()) {
				String prefix = name.substring(0, m.start());
				String suffix = name.substring(m.end());
				if (m.end() - m.start() > 2) {
					int index = Integer.parseInt(name.substring(m.start() + 1, m.end() - 1));
					JSONArray repeatJa = jo.getJSONArray(prefix);
					for (int i = 0; i < repeatJa.length(); i++) {
						JSONObject repeatJo = repeatJa.getJSONObject(i);
						if (repeatJo.getInt("__index__") == index) {
							addFile(suffix, data, repeatJo);
							break;
						}
					}
				} else {
					addFile(suffix, data, jo.getJSONObject(prefix));
				}
			} else {
				template = template.replaceAll("(\\$[^\\s]*)" + name + "([\\}'\"\\s]+)($|[^i]|i[^n]|in[^\\s])", "$1" + name + ".path$2$3");

				String fileName = data.getName();
				JSONObject fileJo = new JSONObject();
				fileJo.putOpt("path", fileName);
				fileJo.putOpt("name", fileName.split("\\.(?=[^\\.]+$)")[0]);
				fileJo.putOpt("files_path", data.getLocalDirectory());

				if (data.hasDataType()) {
					GalaxyDataType dataType = data.getDataType();
					String fileExt = dataType.getExtension();
					fileJo.putOpt("extension", fileExt);
					fileJo.putOpt("ext", fileExt);
				}

				if (data.hasDataType())
					fileJo.putOpt("metadata", new JSONObject());

				jo.putOpt(name, fileJo);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public void addParam(String name, GalaxyParam param) {
		params.add(param);
	}

	public void addRequirement(String name, String version) {
		requirements.put(name, version);
	}

	public String getEnv() {
		return env;
	}

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

	public String getName() {
		return getId() + "/" + getVersion();
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

	public String getVersion() {
		return version;
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	private void mapParams(JSONObject jo) throws JSONException {
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

	public void populateToolState(JSONObject toolState) throws JSONException {
		mapParams(toolState);
		toolState.put("__new_file_path__", ".");
	}

	public void setParams(Set<GalaxyParam> params) {
		this.params = params;
	}

	public void setTemplate(String template) {
		this.template = template.endsWith("\n") ? template : template + "\n";
	}

	@Override
	public String toString() {
		return getName();
	}
}
