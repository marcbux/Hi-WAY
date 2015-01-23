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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;
import de.huberlin.wbi.hiway.am.HiWay;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;

/**
 * An object that extends the Hi-WAY task instance object with its Galaxy tool and tool state, along with methods and structures to utilize and populate them.
 * 
 * @author Marc Bux
 *
 */
public class GalaxyTaskInstance extends TaskInstance {
	// the Galaxy tool invoked by this task instance
	private GalaxyTool galaxyTool;
	// the tool state (i.e., the parameter settings in JSON format) of this task instance
	private JSONObject toolState;
	// input data, which the task instance has to know about for determining the metadata using the Python classes provided by Galaxy
	private Set<GalaxyData> inputs;
	// the Python script run before the actual task instance execution that is responsible for populating the tool state with metadata
	private StringBuilder paramScript;
	// the post script that is to be run subsequent to the actual task invocation and that may involve the moving of files from temporary fodlers to their
	// destination
	private String postScript;

	public GalaxyTaskInstance(long id, String taskName, GalaxyTool galaxyTool) {
		super(id, UUID.randomUUID(), taskName, Math.abs(taskName.hashCode()), ForeignLambdaExpr.LANGID_BASH);
		this.galaxyTool = galaxyTool;
		toolState = new JSONObject();
		paramScript = new StringBuilder("import os, ast\nimport cPickle as pickle\nimport galaxy.app\n");
		this.postScript = "";
		inputs = new HashSet<>();

		// the task instance's invocScript variable is set here for it to be passed to the Worker thread, which writes the script's content as JsonReportEntry
		// to the stat.log
		setInvocScript(id + ".sh");

		// As opposed to other Hi-WAY applciation masters, the Galaxy AM ha a fairly static command that can be build at task instance creation time
		StringBuilder commandSb = new StringBuilder();
		commandSb.append("python ").append(id).append(".params.py\n");
		commandSb.append("cat ").append(id).append(".pre.sh > ").append(id).append(".sh\n");
		commandSb.append("echo `cheetah fill ").append(id).append(".template.tmpl --pickle ").append(id).append(".pickle.p -p` >> ").append(id).append(".sh\n");
		commandSb.append("cat ").append(id).append(".post.sh >> ").append(id).append(".sh\n");
		commandSb.append("bash ").append(getInvocScript()).append("\n");
		setCommand(commandSb.toString());
	}

	/**
	 * A method that is called when a file name is encountered in the workflow description and the tool state has to be adjusted accordingly
	 * 
	 * @param name
	 *            the parameter name for this file, as specified in the workflow description
	 * @param computeMetadata
	 *            a parameter that determines whether metadata is to be computed for this file, which will only be the case for input data
	 * @param data
	 *            the Hi-WAY data object for this file
	 */
	public void addFile(String name, boolean computeMetadata, GalaxyData data) {
		addFile(name, computeMetadata, data, toolState);
	}

	/**
	 * A (recursive) method that is passed a file along with the JSON object in this task instance's tool state that corresponds to this file. This method is
	 * used to populate the tool state with information on the file.
	 * 
	 * @param name
	 *            the parameter name for this file, as specified in the workflow description
	 * @param computeMetadata
	 *            a parameter that determines whether metadata is to be computed for this file, which will only be the case for input data
	 * @param data
	 *            the Hi-WAY data object for this file
	 * @param jo
	 *            the JSON object in the tool state that corresponds to this file parameter
	 */
	private void addFile(String name, boolean computeMetadata, GalaxyData data, JSONObject jo) {
		try {
			Pattern p = Pattern.compile("(_[0-9]*)?\\|");
			Matcher m = p.matcher(name);

			// (1) if the to-be-added file is part of a repeat (and thus can be identified by an underscore and index number in its name), compute its prefix
			// (the repeat name) as well as its suffix (the actual parameter name) and index; use the prefix and index to obtain its JSON object from the tool
			// state and (recursively) call this method to proceed to (2)
			if (m.find()) {
				String prefix = name.substring(0, m.start());
				String suffix = name.substring(m.end());
				if (m.end() - m.start() > 2) {
					int index = Integer.parseInt(name.substring(m.start() + 1, m.end() - 1));
					JSONArray repeatJa = jo.getJSONArray(prefix);
					for (int i = 0; i < repeatJa.length(); i++) {
						JSONObject repeatJo = repeatJa.getJSONObject(i);
						if (repeatJo.getInt("__index__") == index) {
							addFile(suffix, computeMetadata, data, repeatJo);
							break;
						}
					}
				} else {
					addFile(suffix, computeMetadata, data, jo.getJSONObject(prefix));
				}
				// (2) fix both the template of this tool and the tool state of the task instance calling this method, such that they are in compliance with one
				// another when the tool state is used to set the parameters of the task instance at execution time
			} else {
				// (a) adjust the galaxy tool's template such that it is conform with the tool state
				galaxyTool.addFile(name);

				// (b) add several properties for this parameter to the tool state
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
				// note that this metadata is an empty dictionary that will only be be filled for input data of the task instance calling this method; this is
				// done by executing a python script designed to populate the tool state with metadata prior to execution
				if (data.hasDataType())
					fileJo.putOpt("metadata", new JSONObject());
				jo.putOpt(name, fileJo);

				// (c) adjust the Python script for setting parameters at runtime to compute metadata for this file, given the computeMetadata parameter is set
				if (computeMetadata && data.hasDataType()) {
					GalaxyDataType dataType = data.getDataType();
					paramScript.append("from ");
					paramScript.append(dataType.getFile());
					paramScript.append(" import ");
					paramScript.append(dataType.getName());
					paramScript.append("\n");
					inputs.add(data);
				}
			}
		} catch (JSONException e) {
			HiWay.onError(e);
		}
	}

	/**
	 * A method for setting the tool state of this task instance from the raw string of the workflow description file to the JSON object provided in the Python
	 * parameter script. There are several string operations that have to be performed before the tool state can be interpreted as a JSON object.
	 * 
	 * @param toolState
	 *            the tool state string, as extracted from the workflow description file
	 * 
	 */
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
			HiWay.onError(e);
		}
	}

	public void addToPostScript(String post) {
		postScript = postScript + (post.endsWith("\n") ? post : post + "\n");
	}

	@Override
	public Map<String, LocalResource> buildScriptsAndSetResources(FileSystem fs, Container container) {
		Map<String, LocalResource> localResources = super.buildScriptsAndSetResources(fs, container);
		String containerId = container.getId().toString();

		Data preSriptData = new Data(id + ".pre.sh");
		Data paramScriptData = new Data(id + ".params.py");
		Data templateData = new Data(id + ".template.tmpl");
		Data postSriptData = new Data(id + ".post.sh");

		try (BufferedWriter preScriptWriter = new BufferedWriter(new FileWriter(preSriptData.getLocalPath()));
				BufferedWriter paramScriptWriter = new BufferedWriter(new FileWriter(paramScriptData.getLocalPath()));
				BufferedWriter templateWriter = new BufferedWriter(new FileWriter(templateData.getLocalPath()));
				BufferedWriter postScriptWriter = new BufferedWriter(new FileWriter(postSriptData.getLocalPath()))) {
			preScriptWriter.write(galaxyTool.getEnv());
			paramScriptWriter.write(paramScript.toString());
			templateWriter.write(galaxyTool.getTemplate());
			postScriptWriter.write(getPostScript());

			preSriptData.addToLocalResourceMap(localResources, fs, containerId);
			paramScriptData.addToLocalResourceMap(localResources, fs, containerId);
			templateData.addToLocalResourceMap(localResources, fs, containerId);
			postSriptData.addToLocalResourceMap(localResources, fs, containerId);
		} catch (IOException e) {
			HiWay.onError(e);
		}

		return localResources;
	}

	public GalaxyTool getGalaxyTool() {
		return galaxyTool;
	}

	public String getPostScript() {
		return postScript;
	}

	/**
	 * A method for preparing the Python script that is responsible for populating the tool state with metadata and making it accessible for Cheetah to compile
	 * the template.
	 * 
	 * @throws JSONException
	 */
	public void prepareParamScript() throws JSONException {
		galaxyTool.populateToolState(toolState);
		paramScript.append("\ntool_state = ");
		paramScript.append(toolState.toString());
		paramScript.append("\n\nclass Dict(dict):");
		paramScript.append("\n    def __init__(self, *args, **kwargs):");
		paramScript.append("\n        super(Dict, self).__init__(*args, **kwargs)");
		paramScript.append("\n        self.__dict__ = self");
		paramScript.append("\n\nclass Dataset(Dict):");
		paramScript.append("\n    def has_data(self):");
		paramScript.append("\n        return True");
		paramScript.append("\n    def get_size(self):");
		paramScript.append("\n        return os.path.getsize(self.file_name)");
		paramScript.append("\n\ndef expandToolState(src, dest):");
		paramScript.append("\n    for k, v in src.iteritems():");
		paramScript.append("\n        if isinstance (v, dict):");
		paramScript.append("\n            dest[k] = Dataset() if 'path' in v else Dict()");
		paramScript.append("\n            expandToolState(v, dest[k])");
		for (GalaxyData input : inputs) {
			paramScript.append("\n            if 'path' in v and v['path'] == '");
			paramScript.append(input.getName());
			paramScript.append("':");
			paramScript.append("\n                dest[k]['file_name'] = v['path']");
			paramScript.append("\n                datatype = ");
			paramScript.append(input.getDataType().getName());
			paramScript.append("()");
			paramScript.append("\n                for key in datatype.metadata_spec.keys():");
			paramScript.append("\n                    value = datatype.metadata_spec[key]");
			paramScript.append("\n                    default = value.get(\"default\")");
			paramScript.append("\n                    dest[k].metadata[key] = default");
			paramScript.append("\n                try:");
			paramScript.append("\n                    datatype.set_meta(dataset=dest[k])");
			paramScript.append("\n                except:");
			paramScript.append("\n                    pass");
			paramScript.append("\n                for key in dest[k].metadata.keys():");
			paramScript.append("\n                    value = dest[k].metadata[key]");
			paramScript.append("\n                    if isinstance (value, list):");
			paramScript.append("\n                        dest[k].metadata[key] = ', '.join(str(item) for item in value)");
		}
		paramScript.append("\n        elif isinstance (v, list):");
		paramScript.append("\n            dest[k] = list()");
		paramScript.append("\n            for i in v:");
		paramScript.append("\n                j = Dict()");
		paramScript.append("\n                dest[k].append(j)");
		paramScript.append("\n                expandToolState(i, j)");
		paramScript.append("\n        else:");
		paramScript.append("\n            dest[k] = v");
		paramScript.append("\n\nexpanded_tool_state = Dict()");
		paramScript.append("\nexpandToolState(tool_state, expanded_tool_state)");
		paramScript.append("\nwith open(\"");
		paramScript.append(id);
		paramScript.append(".pickle.p\", \"wb\") as picklefile:");
		paramScript.append("\n    pickle.dump(ast.literal_eval(str(expanded_tool_state)), picklefile)\n");
	}

}
