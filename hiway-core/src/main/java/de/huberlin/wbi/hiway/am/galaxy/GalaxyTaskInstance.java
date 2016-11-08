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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;
import de.huberlin.wbi.hiway.am.WorkflowDriver;
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
	// input data, which the task instance has to know about for determining the metadata using the Python classes provided by Galaxy
	private Set<GalaxyData> inputs;
	// the Python script run before the actual task instance execution that is responsible for populating the tool state with metadata
	private StringBuilder paramScript;
	// the post script that is to be run subsequent to the actual task invocation and that may involve the moving of files from temporary fodlers to their
	// destination
	private String postScript;
	// the tool state (i.e., the parameter settings in JSON format) of this task instance
	private JSONObject toolState;

	public GalaxyTaskInstance(long id, UUID workflowId, String taskName, GalaxyTool galaxyTool, String galaxyPath) {
		super(id, workflowId, taskName, Math.abs(taskName.hashCode() + 1), ForeignLambdaExpr.LANGID_BASH);
		this.galaxyTool = galaxyTool;
		toolState = new JSONObject();
		paramScript = new StringBuilder("import os, ast\nimport cPickle as pickle\nimport galaxy.app\n");
		this.postScript = "";
		inputs = new HashSet<>();

		// the task instance's invocScript variable is set here for it to be passed to the Worker thread, which writes the script's content as JsonReportEntry
		// to the log
		setInvocScript("script.sh");

		// As opposed to other Hi-WAY applciation masters, the Galaxy AM ha a fairly static command that can be build at task instance creation time
		StringBuilder commandSb = new StringBuilder();
		commandSb.append("PYTHONPATH=" + galaxyPath + "/lib:$PYTHONPATH; export PYTHONPATH\n");
		commandSb.append("PYTHON_EGG_CACHE=.; export PYTHON_EGG_CACHE\n");
		commandSb.append(". " + galaxyPath + "/.venv/bin/activate\n");
		commandSb.append("python params_" + id + ".py\n");
		commandSb.append("deactivate\n");
		commandSb.append("cat pre_" + id + ".sh > script.sh\n");
		commandSb.append("echo `cheetah fill template_" + id + ".tmpl --pickle params.p -p` >> script.sh\n");
		commandSb.append("cat post_" + id + ".sh >> script.sh\n");
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
				// (a) add several properties for this parameter to the tool state
				String fileName = data.getName();
				JSONObject fileJo = new JSONObject();
				fileJo.putOpt("path", fileName);
				fileJo.putOpt("name", fileName.split("\\.(?=[^\\.]+$)")[0]);
				Path dir = data.getLocalPath().getParent();
				String dirString = (dir == null) ? "" : dir.toString();
				if (dirString.length() == 0) {
					dirString = ".";
				}
				fileJo.putOpt("files_path", dirString);
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

				// (b) adjust the Python script for setting parameters at runtime to compute metadata for this file, given the computeMetadata parameter is set
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
			e.printStackTrace(System.out);
			System.exit(-1);
		}
	}

	/**
	 * A method for setting the tool state of this task instance from the raw string of the workflow description file to the JSON object provided in the Python
	 * parameter script. There are several string operations that have to be performed before the tool state can be interpreted as a JSON object.
	 * 
	 * @param tool_state
	 *            the tool state string, as extracted from the workflow description file
	 * 
	 */
	public void addToolState(String tool_state) {
		String tool_state_json = tool_state;
		// replace "{ }" "[ ]" with { } [ ]
		tool_state_json = tool_state_json.replaceAll("\"\\{", "\\{");
		tool_state_json = tool_state_json.replaceAll("\\}\"", "\\}");
		tool_state_json = tool_state_json.replaceAll("\"\\[", "\\[");
		tool_state_json = tool_state_json.replaceAll("\\]\"", "\\]");
		// remove \
		tool_state_json = tool_state_json.replaceAll("\\\\", "");
		// replace "" with "
		tool_state_json = tool_state_json.replaceAll("\"\"", "\"");
		// replace : ", with : "",
		tool_state_json = tool_state_json.replaceAll(": ?\",", ": \"\",");
		// replace UnvalidatedValue with their actual value
		tool_state_json = tool_state_json.replaceAll("\\{\"__class__\":\\s?\"UnvalidatedValue\",\\s?\"value\":\\s?([^\\}]*)\\}", "$1");
		// replace "null" with ""
		tool_state_json = tool_state_json.replaceAll("\"null\"", "\"\"");
		try {
			this.toolState = new JSONObject(tool_state_json);
		} catch (JSONException e) {
			WorkflowDriver.writeToStdout(tool_state_json);
			e.printStackTrace(System.out);
			System.exit(-1);
		}
	}

	/**
	 * A method for adding commands to be executed after the task instance (e.g., for moving files from a temporary folder to their destination, as specified in
	 * the tool description)
	 * 
	 * @param post
	 *            the shell command to be executed after the task instance
	 */
	public void addToPostScript(String post) {
		postScript = postScript + (post.endsWith("\n") ? post : post + "\n");
	}

	@Override
	public Map<String, LocalResource> buildScriptsAndSetResources(Container container) {
		Map<String, LocalResource> localResources = super.buildScriptsAndSetResources(container);
		String containerId = container.getId().toString();

		// The task isntance's bash script is built by appending the pre script, the template compiled by Cheetah using the parameters set in the params Python
		// script, and the post script
		Data preSriptData = new Data("pre_" + getId() + ".sh", containerId);
		Data paramScriptData = new Data("params_" + getId() + ".py", containerId);
		Data templateData = new Data("template_" + getId() + ".tmpl", containerId);
		Data postSriptData = new Data("post_" + getId() + ".sh", containerId);

		try (BufferedWriter preScriptWriter = new BufferedWriter(new FileWriter(preSriptData.getLocalPath().toString()));
				BufferedWriter paramScriptWriter = new BufferedWriter(new FileWriter(paramScriptData.getLocalPath().toString()));
				BufferedWriter templateWriter = new BufferedWriter(new FileWriter(templateData.getLocalPath().toString()));
				BufferedWriter postScriptWriter = new BufferedWriter(new FileWriter(postSriptData.getLocalPath().toString()))) {
			preScriptWriter.write(galaxyTool.getEnv());
			preScriptWriter.write("GALAXY_SLOTS=$(nproc)\n");
			paramScriptWriter.write(paramScript.toString());
			templateWriter.write(galaxyTool.getTemplate());
			postScriptWriter.write(getPostScript());
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		try {
			preSriptData.stageOut();
			paramScriptData.stageOut();
			templateData.stageOut();
			postSriptData.stageOut();

			preSriptData.addToLocalResourceMap(localResources);
			paramScriptData.addToLocalResourceMap(localResources);
			templateData.addToLocalResourceMap(localResources);
			postSriptData.addToLocalResourceMap(localResources);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
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
	 *             JSONException
	 */
	public void prepareParamScript() throws JSONException {
		// (1) populate the tool state by mapping parameters and adding additional parameters
		galaxyTool.mapParams(toolState);
		toolState.put("__new_file_path__", ".");
		// (2) Set the tool state to the tool state as parsed from the workflow description and populated in step 1
		paramScript.append("\ntool_state = ");
		paramScript.append(toolState.toString());
		// (3) Define the Dict class, which is a Python dict whose members are accessible like functions of a class (via a "dot")
		paramScript.append("\n\nclass Dict(dict):");
		paramScript.append("\n    def __init__(self, *args, **kwargs):");
		paramScript.append("\n        super(Dict, self).__init__(*args, **kwargs)");
		paramScript.append("\n        self.__dict__ = self");
		// (4) The Dataset class inherits from the Dict class and provides some functions; in order to utilize the Python scripts describing the Galaxy data
		// types, a Dataset object has to be created; unfortunately, the Dataset object provided by Galaxy requires Galaxy to run, hence a custom Dataset class
		// had to be created
		paramScript.append("\n\nclass Dataset(Dict):");
		paramScript.append("\n    def has_data(self):");
		paramScript.append("\n        return True");
		paramScript.append("\n    def get_size(self):");
		paramScript.append("\n        return os.path.getsize(self.file_name)");
		// (5) The expandToolState method traverses the tool state recursively and, when encountering an input data item, determines and adds its metadata
		paramScript.append("\n\ndef expandToolState(src, dest):");
		paramScript.append("\n    for k, v in src.iteritems():");
		// (6) recusrively parse dicts of parameters (conditionals and the root parameter list)
		paramScript.append("\n        if isinstance (v, dict):");
		paramScript.append("\n            dest[k] = Dataset() if 'path' in v else Dict()");
		paramScript.append("\n            expandToolState(v, dest[k])");
		// for each input data item
		for (GalaxyData input : inputs) {
			// (a) instantiate the Python class corresponding to this data item's data type
			paramScript.append("\n            if 'path' in v and v['path'] == '");
			paramScript.append(input.getName());
			paramScript.append("':");
			paramScript.append("\n                dest[k]['file_name'] = v['path']");
			paramScript.append("\n                datatype = ");
			paramScript.append(input.getDataType().getName());
			paramScript.append("()");
			// (b) set the default values of this data item, as defined in its Python class
			paramScript.append("\n                for key in datatype.metadata_spec.keys():");
			paramScript.append("\n                    value = datatype.metadata_spec[key]");
			paramScript.append("\n                    default = value.get(\"default\")");
			paramScript.append("\n                    dest[k].metadata[key] = default");
			paramScript.append("\n                try:");
			// (c) attempt to determine additional metadata, as defined in the set_meta method of the Galaxy data type's Python classes
			paramScript.append("\n                    datatype.set_meta(dataset=dest[k])");
			paramScript.append("\n                except:");
			paramScript.append("\n                    pass");
			// (d) metadata information in Python list format has to converted to a string and formatted before it can be added to the tool state
			paramScript.append("\n                for key in dest[k].metadata.keys():");
			paramScript.append("\n                    value = dest[k].metadata[key]");
			paramScript.append("\n                    if isinstance (value, list):");
			paramScript.append("\n                        dest[k].metadata[key] = ', '.join(str(item) for item in value)");
		}
		// (7) recursively parse lists of parameters (repeats)
		paramScript.append("\n        elif isinstance (v, list):");
		paramScript.append("\n            dest[k] = list()");
		paramScript.append("\n            for i in v:");
		paramScript.append("\n                j = Dict()");
		paramScript.append("\n                dest[k].append(j)");
		paramScript.append("\n                expandToolState(i, j)");
		// (8) if the encountered item is neither of type dict nor of type list (and hence atomic), just add it to the expanded dest dict
		paramScript.append("\n        else:");
		paramScript.append("\n            dest[k] = v");
		// (9) invoke the expandToolState method and write its result to a pickle file, which Cheetah can interpret to compile the template
		paramScript.append("\n\nexpanded_tool_state = Dict()");
		paramScript.append("\nexpandToolState(tool_state, expanded_tool_state)");
		paramScript.append("\nwith open(\"params.p\", \"wb\") as picklefile:");
		paramScript.append("\n    pickle.dump(ast.literal_eval(str(expanded_tool_state)), picklefile)\n");
	}

}
