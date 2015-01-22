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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;
import de.huberlin.wbi.hiway.common.TaskInstance;

public class GalaxyTaskInstance extends TaskInstance {
	private GalaxyTool galaxyTool;
	Set<GalaxyData> inputs;
	StringBuilder pickleScript;
	private String postScript;

	private JSONObject toolState;

	public GalaxyTaskInstance(long id, String taskName, GalaxyTool galaxyTool) {
		super(id, UUID.randomUUID(), taskName, Math.abs(taskName.hashCode()), ForeignLambdaExpr.LANGID_BASH);
		this.galaxyTool = galaxyTool;
		toolState = new JSONObject();
		pickleScript = new StringBuilder("import os, ast\nimport cPickle as pickle\nimport galaxy.app\n");
		this.postScript = "";
		inputs = new HashSet<>();
	}

	public void addFile(String name, boolean computeMetadata, GalaxyData data) {
		galaxyTool.addFile(name, data, toolState);
		if (computeMetadata && data.hasDataType()) {
			GalaxyDataType dataType = data.getDataType();
			pickleScript.append("from ");
			pickleScript.append(dataType.getFile());
			pickleScript.append(" import ");
			pickleScript.append(dataType.getName());
			pickleScript.append("\n");
			inputs.add(data);
		}
	}

	public void addToolState(String toolState) {
		String toolState_json = toolState;
		// replace "{ }" "[ ]" with { } [ ]
		toolState_json = toolState_json.replaceAll("\"\\{", "\\{");
		toolState_json = toolState_json.replaceAll("\\}\"", "\\}");
		toolState_json = toolState_json.replaceAll("\"\\[", "\\[");
		toolState_json = toolState_json.replaceAll("\\]\"", "\\]");
		// remove \
		toolState_json = toolState_json.replaceAll("\\\\", "");
		// replace "" with "
		toolState_json = toolState_json.replaceAll("\"\"", "\"");
		// replace : ", with : "",
		toolState_json = toolState_json.replaceAll(": ?\",", ": \"\",");
		// replace UnvalidatedValue with their actual value
		toolState_json = toolState_json.replaceAll("\\{\"__class__\":\\s?\"UnvalidatedValue\",\\s?\"value\":\\s?([^\\}]*)\\}", "$1");
		// replace "null" with ""
		toolState_json = toolState_json.replaceAll("\"null\"", "\"\"");
		try {
			this.toolState = new JSONObject(toolState_json);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public void addToPostScript(String post) {
		postScript = postScript + (post.endsWith("\n") ? post : post + "\n");
	}

	public void buildPickleScript() throws JSONException {
		galaxyTool.populateToolState(toolState);
		pickleScript.append("\ntool_state = ");
		pickleScript.append(toolState.toString());
		pickleScript.append("\n\nclass Dict(dict):");
		pickleScript.append("\n    def __init__(self, *args, **kwargs):");
		pickleScript.append("\n        super(Dict, self).__init__(*args, **kwargs)");
		pickleScript.append("\n        self.__dict__ = self");
		pickleScript.append("\n\nclass Dataset(Dict):");
		pickleScript.append("\n    def has_data(self):");
		pickleScript.append("\n        return True");
		pickleScript.append("\n    def get_size(self):");
		pickleScript.append("\n        return os.path.getsize(self.file_name)");
		pickleScript.append("\n\ndef expandToolState(src, dest):");
		pickleScript.append("\n    for k, v in src.iteritems():");
		pickleScript.append("\n        if isinstance (v, dict):");
		pickleScript.append("\n            dest[k] = Dataset() if 'path' in v else Dict()");
		pickleScript.append("\n            expandToolState(v, dest[k])");
		for (GalaxyData input : inputs) {
			pickleScript.append("\n            if 'path' in v and v['path'] == '");
			pickleScript.append(input.getName());
			pickleScript.append("':");
			pickleScript.append("\n                dest[k]['file_name'] = v['path']");
			pickleScript.append("\n                datatype = ");
			pickleScript.append(input.getDataType().getName());
			pickleScript.append("()");
			pickleScript.append("\n                for key in datatype.metadata_spec.keys():");
			pickleScript.append("\n                    value = datatype.metadata_spec[key]");
			pickleScript.append("\n                    default = value.get(\"default\")");
			pickleScript.append("\n                    dest[k].metadata[key] = default");
			pickleScript.append("\n                try:");
			pickleScript.append("\n                    datatype.set_meta(dataset=dest[k])");
			pickleScript.append("\n                except:");
			pickleScript.append("\n                    pass");
			pickleScript.append("\n                for key in dest[k].metadata.keys():");
			pickleScript.append("\n                    value = dest[k].metadata[key]");
			pickleScript.append("\n                    if isinstance (value, list):");
			pickleScript.append("\n                        dest[k].metadata[key] = ', '.join(str(item) for item in value)");
		}
		pickleScript.append("\n        elif isinstance (v, list):");
		pickleScript.append("\n            dest[k] = list()");
		pickleScript.append("\n            for i in v:");
		pickleScript.append("\n                j = Dict()");
		pickleScript.append("\n                dest[k].append(j)");
		pickleScript.append("\n                expandToolState(i, j)");
		pickleScript.append("\n        else:");
		pickleScript.append("\n            dest[k] = v");
		pickleScript.append("\n\nexpanded_tool_state = Dict()");
		pickleScript.append("\nexpandToolState(tool_state, expanded_tool_state)");
		pickleScript.append("\nwith open(\"");
		pickleScript.append(id);
		pickleScript.append(".pickle.p\", \"wb\") as picklefile:");
		pickleScript.append("\n    pickle.dump(ast.literal_eval(str(expanded_tool_state)), picklefile)\n");
		try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(id + ".params.py"))) {
			scriptWriter.write(pickleScript.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void buildTemplate() {
		String preScript = galaxyTool.getEnv();
		String template = galaxyTool.getTemplate();
		String postScript = getPostScript();

		try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(id + ".pre.sh"))) {
			scriptWriter.write(preScript);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(id + ".template.tmpl"))) {
			scriptWriter.write(template);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(id + ".post.sh"))) {
			scriptWriter.write(postScript);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public GalaxyTool getGalaxyTool() {
		return galaxyTool;
	}

	public String getPostScript() {
		return postScript;
	}

}
