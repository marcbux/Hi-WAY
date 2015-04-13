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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.am.HiWay;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class GalaxyApplicationMaster extends HiWay {

	public static void main(String[] args) {
		HiWay.loop(new GalaxyApplicationMaster(), args);
	}

	/**
	 * A helper function for processing the loc file of a single Galaxy data table; the loc file stores information on any registered data (e.g., genomic
	 * indices)
	 * 
	 * @param file
	 *            a data table's loc file
	 * @param galaxyDataTable
	 *            the data table object corresponding to this loc file
	 */
	private static void processLocFile(File file, GalaxyDataTable galaxyDataTable) {
		if (!file.exists())
			return;
		try (BufferedReader locBr = new BufferedReader(new FileReader(file))) {
			System.out.println("Processing Galaxy data table loc file " + file.getCanonicalPath());
			String line;
			while ((line = locBr.readLine()) != null) {
				if (line.startsWith(galaxyDataTable.getComment_char()))
					continue;
				String[] content = line.split("\t");
				galaxyDataTable.addContent(content);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/* a data structure that stores the data tables of the local Galaxy installation; data tables contain references to installed data sets (e.g., genome
	 * indices) */
	private Map<String, GalaxyDataTable> galaxyDataTables;
	/* a data structure that stores the data types of the local Galaxy installation; data types are associated with metadata information that is required to
	 * invoke the tools provided by Galaxy */
	private Map<String, GalaxyDataType> galaxyDataTypes;
	/* the path of the local Galaxy installation, as specifified in the hiway-site.xml */
	public final String galaxyPath;
	/* a data structure that stores the tools of the local Galaxy installation; these tools include the library tools pre-installed in Galaxy as well as
	 * additional tools installed via Galaxy's tool shed functionality */
	private Map<String, Map<String, GalaxyTool>> galaxyTools;

	public GalaxyApplicationMaster() {
		super();
		galaxyPath = getConf().get(HiWayConfiguration.HIWAY_GALAXY_PATH);
		if (galaxyPath == null) {
			System.err.println(HiWayConfiguration.HIWAY_GALAXY_PATH + " not set in  " + HiWayConfiguration.HIWAY_SITE_XML);
			throw new RuntimeException();
		}
		galaxyDataTables = new HashMap<>();
		galaxyDataTypes = new HashMap<>();
		galaxyTools = new HashMap<>();
		setDetermineFileSizes();
	}

	/**
	 * A helper function for setting and obtaining tools from their data structure
	 * 
	 * @param id
	 *            the identifier of the tool, as specified in the root element of the tool's description XML
	 * @return a map storing instances of the tool by their version number
	 */
	private Map<String, GalaxyTool> addAndGetToolMap(String id) {
		if (!galaxyTools.containsKey(id)) {
			Map<String, GalaxyTool> toolMap = new HashMap<>();
			galaxyTools.put(id, toolMap);
		}
		return galaxyTools.get(id);
	}

	/**
	 * A (recursive) helper function for parsing the parameters of a tool from their XML specification
	 * 
	 * @param el
	 *            the XML element from which to commence the parsing
	 * @return the set of parameters under this element
	 * @throws XPathExpressionException
	 */
	private Set<GalaxyParam> getParams(Element el, GalaxyTool tool) throws XPathExpressionException {
		Set<GalaxyParam> params = new HashSet<>();
		XPath xpath = XPathFactory.newInstance().newXPath();

		// there are three different types of parameters in Galaxy's tool descriptions: atomic parameters, conditionals and repeats
		NodeList paramNds = (NodeList) xpath.evaluate("param", el, XPathConstants.NODESET);
		NodeList conditionalNds = (NodeList) xpath.evaluate("conditional", el, XPathConstants.NODESET);
		NodeList repeatNds = (NodeList) xpath.evaluate("repeat", el, XPathConstants.NODESET);

		// (1) parse atomic parameters
		for (int i = 0; i < paramNds.getLength(); i++) {
			Element paramEl = (Element) paramNds.item(i);
			String name = paramEl.getAttribute("name");
			GalaxyParamValue param = new GalaxyParamValue(name);
			params.add(param);

			// (a) determine default values and mappings of values
			String type = paramEl.getAttribute("type");
			switch (type) {
			case "data":
				param.addMapping("", "{\"path\": \"\"}");
				tool.setPath(name);
				break;
			case "boolean":
				String trueValue = paramEl.getAttribute("truevalue");
				param.addMapping("True", trueValue);
				String falseValue = paramEl.getAttribute("falsevalue");
				param.addMapping("False", falseValue);
				break;
			case "select":
				param.addMapping("", "None");
				break;
			default:
			}

			// (b) resolve references to Galaxy data tables
			NodeList optionNds = (NodeList) xpath.evaluate("option", paramEl, XPathConstants.NODESET);
			NodeList optionsNds = (NodeList) xpath.evaluate("options", paramEl, XPathConstants.NODESET);
			for (int j = 0; j < optionNds.getLength() + optionsNds.getLength(); j++) {
				Element optionEl = j < optionNds.getLength() ? (Element) optionNds.item(j) : (Element) optionsNds.item(j - optionNds.getLength());
				if (optionEl.hasAttribute("from_data_table")) {
					String tableName = optionEl.getAttribute("from_data_table");
					GalaxyDataTable galaxyDataTable = galaxyDataTables.get(tableName);
					for (String value : galaxyDataTable.getValues()) {
						param.addMapping(value, galaxyDataTable.getContent(value));
					}
				}
			}
		}

		// (2) parse conditionals, which consist of a single condition parameter and several "when condition equals" parameters
		for (int i = 0; i < conditionalNds.getLength(); i++) {
			Element conditionalEl = (Element) conditionalNds.item(i);
			String name = conditionalEl.getAttribute("name");
			GalaxyConditional conditional = new GalaxyConditional(name);

			NodeList conditionNds = (NodeList) xpath.evaluate("param", conditionalEl, XPathConstants.NODESET);
			NodeList whenNds = (NodeList) xpath.evaluate("when", conditionalEl, XPathConstants.NODESET);
			if (conditionNds.getLength() == 0 || whenNds.getLength() == 0)
				continue;

			Element conditionEl = (Element) conditionNds.item(0);
			name = conditionEl.getAttribute("name");
			GalaxyParamValue condition = new GalaxyParamValue(name);
			conditional.setCondition(condition);

			for (int j = 0; j < whenNds.getLength(); j++) {
				Element whenEl = (Element) whenNds.item(j);
				String conditionValue = whenEl.getAttribute("value");
				conditional.setConditionalParams(conditionValue, getParams(whenEl, tool));
			}

			params.add(conditional);
		}

		// (3) parse repeats, which consist of a list of parameters
		for (int i = 0; i < repeatNds.getLength(); i++) {
			Element repeatEl = (Element) repeatNds.item(i);
			String name = repeatEl.getAttribute("name");
			GalaxyRepeat repeat = new GalaxyRepeat(name);
			params.add(repeat);

			repeat.setParams(getParams(repeatEl, tool));
		}

		return params;
	}

	@Override
	public boolean init(String[] args) throws ParseException {
		super.init(args);

		// (1) determine the config files that are to be parsed
		String tool_data_table_config_path = "config/tool_data_table_conf.xml.sample";
		String shed_tool_data_table_config = "config/shed_tool_data_table_conf.xml.sample";
		String tool_dependency_dir = "dependencies";
		String tool_path = "tools";
		String tool_config_file = "config/tool_conf.xml.sample";
		String datatypes_config_file = "config/datatypes_conf.xml.sample";
		try (BufferedReader iniBr = new BufferedReader(new FileReader(new File(galaxyPath + "/config/galaxy.ini")))) {
			String line;
			while ((line = iniBr.readLine()) != null) {
				if (line.startsWith("tool_data_table_config_path"))
					tool_data_table_config_path = line.split("=")[1].trim();
				if (line.startsWith("shed_tool_data_table_config"))
					shed_tool_data_table_config = line.split("=")[1].trim();
				if (line.startsWith("tool_dependency_dir"))
					tool_dependency_dir = line.split("=")[1].trim();
				if (line.startsWith("tool_path"))
					tool_path = line.split("=")[1].trim();
				if (line.startsWith("tool_config_file"))
					tool_config_file = line.split("=")[1].trim();
				if (line.startsWith("datatypes_config_file"))
					datatypes_config_file = line.split("=")[1].trim();
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		String[] tool_config_files = tool_config_file.split(",");

		// (2) parse the config files for Galaxy's data types, data tables, and tool libraries
		try {
			processDataTypes(new File(galaxyPath + "/" + datatypes_config_file));
			processDataTables(new File(galaxyPath + "/" + tool_data_table_config_path));
			processDataTables(new File(galaxyPath + "/" + shed_tool_data_table_config));
			for (String config_file : tool_config_files) {
				processToolLibraries(new File(galaxyPath + "/" + config_file.trim()), tool_path, tool_dependency_dir);
			}
		} catch (FactoryConfigurationError e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return true;
	}

	/**
	 * A helper function for parsing a Galaxy tool's XML file
	 * 
	 * @param file
	 *            the XML file to be parsed
	 * @return the Galaxy tools described in the XML file
	 */
	private GalaxyTool parseToolFile(File file) {
		System.out.println("Parsing Galaxy tool file " + file);
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			String path = file.getCanonicalPath();
			String dir = path.substring(0, path.lastIndexOf("/"));
			Document doc = builder.parse(file);
			Element rootEl = doc.getDocumentElement();
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			StreamResult result = new StreamResult(new StringWriter());
			DOMSource source = new DOMSource(rootEl);
			transformer.transform(source, result);
			String toolDescription = result.getWriter().toString();

			// (1) parse macros, if any
			NodeList macrosNds = rootEl.getElementsByTagName("macros");
			Map<String, String> macrosByName = new HashMap<>();
			for (int i = 0; i < macrosNds.getLength(); i++) {
				Node macrosNd = macrosNds.item(i);
				macrosByName.putAll(processMacros(macrosNd, dir));
			}

			// (2) insert macros into the XML and parse the document
			Pattern p = Pattern.compile("<expand macro=\"([^\"]*)\"(>.*?</expand>|/>)", Pattern.DOTALL);
			Matcher m = p.matcher(toolDescription);
			while (m.find()) {
				String name = m.group(1);
				String replace = m.group(0);
				String with = macrosByName.get(name);
				if (m.group(2).startsWith(">")) {
					String yield = m.group(2).substring(1, m.group(2).indexOf("</expand>"));
					with = with.replaceAll("<yield/>", yield.trim());
				}
				if (with != null)
					toolDescription = toolDescription.replace(replace, with);
			}

			doc = builder.parse(new InputSource(new StringReader(toolDescription)));
			rootEl = doc.getDocumentElement();
			String version = rootEl.hasAttribute("version") ? rootEl.getAttribute("version") : "1.0.0";
			String id = rootEl.getAttribute("id");
			GalaxyTool tool = new GalaxyTool(id, version, dir, galaxyPath);

			// (3) determine requirements (libraries and executables) of this tool; requirements have to be parsed such that the environment of the task can be
			// set to include them
			NodeList requirementNds = rootEl.getElementsByTagName("requirement");
			for (int i = 0; i < requirementNds.getLength(); i++) {
				Element requirementEl = (Element) requirementNds.item(i);
				String requirementName = requirementEl.getChildNodes().item(0).getNodeValue().trim();
				String requirementVersion = requirementEl.getAttribute("version");
				tool.addRequirement(requirementName, requirementVersion);
			}

			// (4) determine and set the template for the command of the task; this template will be compiled at runtime by Cheetah
			Element commandEl = (Element) rootEl.getElementsByTagName("command").item(0);
			if (commandEl != null) {
				String command = commandEl.getChildNodes().item(0).getNodeValue().trim();
				String script = command.split(" ")[0];
				String interpreter = commandEl.getAttribute("interpreter");
				if (interpreter.length() > 0) {
					command = command.replace(script, dir + "/" + script);
					command = interpreter + " " + command;
				}
				command = command.replaceAll("\\.value", "");
				command = command.replaceAll("\\.dataset", "");
				tool.setTemplate(command);
			}

			// (5) determine the parameters (atomic, conditional and repeat) of this tool
			Element inputsEl = (Element) rootEl.getElementsByTagName("inputs").item(0);
			if (inputsEl != null)
				tool.setParams(getParams(inputsEl, tool));

			// (6) determine the output files produced by this tool
			Element outputsEl = (Element) rootEl.getElementsByTagName("outputs").item(0);
			if (outputsEl != null) {
				NodeList dataNds = outputsEl.getElementsByTagName("data");
				for (int i = 0; i < dataNds.getLength(); i++) {
					Element dataEl = (Element) dataNds.item(i);
					String name = dataEl.getAttribute("name");
					GalaxyParamValue param = new GalaxyParamValue(name);
					tool.setPath(name);
					tool.addParam(param);

					String format = dataEl.getAttribute("format");
					String metadata_source = dataEl.getAttribute("metadata_source");
					if (format.equals("input") && metadata_source != null && metadata_source.length() > 0) {
						param.setDataType(metadata_source);
					} else {
						param.setDataType(format);
					}

					String from_work_dir = dataEl.getAttribute("from_work_dir");
					param.setFrom_work_dir(from_work_dir);
				}
			}

			// (7) register the tool in the Galaxy tool data structure
			if (tool.getTemplate() != null) {
				Map<String, GalaxyTool> toolMap = addAndGetToolMap(id);
				toolMap.put(version, tool);
			}

			return tool;
		} catch (SAXException | IOException | TransformerException | XPathExpressionException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}

	@Override
	public void parseWorkflow() {
		System.out.println("Parsing Galaxy workflow " + getWorkflowFile());
		Map<Long, TaskInstance> tasks = new HashMap<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(getWorkflowFile().getLocalPath().toString()))) {
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
			JSONObject workflow = new JSONObject(sb.toString());
			JSONObject steps = workflow.optJSONObject("steps");

			// (1) First pass: Parse Nodes
			for (int i = 0; i < steps.length(); i++) {
				JSONObject step = steps.optJSONObject(Integer.toString(i));
				long id = step.getLong("id");
				String type = step.getString("type");

				// (a) input nodes are nodes that do not invoke a task, but simply specify where an input file is located
				if (type.equals("data_input")) {
					JSONArray inputs = step.optJSONArray("inputs");
					for (int j = 0; j < inputs.length(); j++) {
						JSONObject input = inputs.optJSONObject(j);
						String name = input.getString("name");
						GalaxyData data = new GalaxyData(name);

						if (name.contains(".")) {
							String extension = name.substring(name.indexOf(".") + 1);
							if (galaxyDataTypes.containsKey(extension)) {
								data.setDataType(galaxyDataTypes.get(extension));
							}
						}

						String idName = id + "_output";
						data.setInput(true);
						getFiles().put(idName, data);
					}

					// (b) tool nodes are the actual nodes comprising the workflow
				} else if (type.equals("tool")) {
					// (i) obtain the tool description and generate a task instance object
					String toolVersion = step.getString("tool_version");
					String toolId = step.getString("tool_id");
					String[] splitId = toolId.split("/");
					if (splitId.length > 2)
						toolId = splitId[splitId.length - 2];
					Map<String, GalaxyTool> tools = galaxyTools.get(toolId);
					if (tools == null) {
						System.err.println("Tool " + toolId + " could not be located in local Galaxy installation.");
						throw new RuntimeException();
					}
					GalaxyTool tool = tools.get(toolVersion);
					if (tool == null) {
						System.err.println("Tool version " + toolVersion + " of tool " + toolId + " could not be located in local Galaxy installation.");
						throw new RuntimeException();
					}
					GalaxyTaskInstance task = new GalaxyTaskInstance(id, getRunId(), tool.getId(), tool, galaxyPath);
					tasks.put(id, task);

					// (ii) determine the and incorporate post job actions apecified in the workflow (e.g., renaming the task's output data)
					Map<String, String> renameOutputs = new HashMap<>();
					Set<String> hideOutputs = new HashSet<>();
					if (step.has("post_job_actions")) {
						JSONObject post_job_actions = step.optJSONObject("post_job_actions");
						for (Iterator<?> it = post_job_actions.keys(); it.hasNext();) {
							JSONObject post_job_action = post_job_actions.optJSONObject((String) it.next());
							String action_type = post_job_action.getString("action_type");
							if (action_type.equals("RenameDatasetAction")) {
								String output_name = post_job_action.getString("output_name");
								JSONObject action_arguments = post_job_action.optJSONObject("action_arguments");
								if (action_arguments != null) {
									String newname = action_arguments.getString("newname");
									if (newname.contains(" "))
										newname = newname.replaceAll("\\s", "_");
									renameOutputs.put(output_name, newname);
								}
							} else if (action_type.equals("HideDatasetAction")) {
								String output_name = post_job_action.getString("output_name");
								hideOutputs.add(output_name);
							}
						}
					}

					// (iii) set the tool state (i.e., the parameter settings) of the task
					task.addToolState(step.getString("tool_state"));

					// (iv) resolve the file names of input data
					Map<String, String> inputNameToIdName = new HashMap<>();
					JSONObject input_connections = step.optJSONObject("input_connections");
					for (String input_name : JSONObject.getNames(input_connections)) {
						JSONObject input_connection = input_connections.optJSONObject(input_name);
						inputNameToIdName.put(input_name, input_connection.getString("id") + "_" + input_connection.getString("output_name"));
					}

					// (v) handle output data
					JSONArray outputs = step.optJSONArray("outputs");
					List<String> outputFiles = new LinkedList<>();
					for (int j = 0; j < outputs.length(); j++) {
						JSONObject output = outputs.optJSONObject(j);
						String outputName = output.getString("name");

						// determine the output file's data type
						GalaxyDataType dataType = null;
						GalaxyParamValue param = tool.getFirstMatchingParamByName(outputName);
						String outputTypeString = param.getDataType();
						if (galaxyDataTypes.containsKey(outputTypeString)) {
							dataType = galaxyDataTypes.get(outputTypeString);
						} else if (inputNameToIdName.containsKey(outputTypeString)) {
							dataType = ((GalaxyData) getFiles().get(inputNameToIdName.get(outputTypeString))).getDataType();
						} else if (outputTypeString.equals("input")) {
							dataType = ((GalaxyData) getFiles().get(inputNameToIdName.values().iterator().next())).getDataType();
						}

						// determine the output file's name
						String fileName = id + "_" + outputName;
						if (dataType != null) {
							String extension = dataType.getExtension();
							if (extension != null && extension.length() > 0) {
								fileName = fileName + "." + extension;
							}
						}
						if (renameOutputs.containsKey(outputName))
							fileName = renameOutputs.get(outputName);

						// if the output file is to moved from a (temporary) working directory, append a command in the task's post script
						if (param.hasFrom_work_dir())
							task.addToPostScript("mv " + param.getFrom_work_dir() + " " + fileName);

						// create the data object and add it to the task object and data structures
						GalaxyData data = new GalaxyData(fileName);
						data.setDataType(dataType);
						String idName = id + "_" + outputName;
						if (!hideOutputs.contains(outputName)) {
							data.setOutput(true);
						}
						getFiles().put(idName, data);
						task.addOutputData(data);
						task.addFile(outputName, false, data);
						outputFiles.add(fileName);
					}

					task.getReport().add(
							new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(), null,
									JsonReportEntry.KEY_INVOC_OUTPUT, new JSONObject().put("output", outputFiles)));

				}
			}

			// (2) Second pass: Parse Edges
			for (int i = 0; i < steps.length(); i++) {
				JSONObject step = steps.optJSONObject(Integer.toString(i));
				long id = step.getLong("id");
				String type = step.getString("type");
				if (type.equals("tool")) {
					GalaxyTaskInstance task = (GalaxyTaskInstance) tasks.get(id);
					JSONObject input_connections = step.optJSONObject("input_connections");
					for (Iterator<?> it = input_connections.keys(); it.hasNext();) {
						String input_connection_key = (String) it.next();
						JSONObject input_connection = input_connections.optJSONObject(input_connection_key);
						long parentId = input_connection.getLong("id");
						String idName = parentId + "_" + input_connection.getString("output_name");

						// (a) register workflow edges
						TaskInstance parentTask = tasks.get(parentId);
						if (parentTask != null) {
							task.addParentTask(parentTask);
							parentTask.addChildTask(task);
						}

						// (b) obtain the data object and add it to the task object
						task.addInputData(getFiles().get(idName));
						task.addFile(input_connection_key, true, (GalaxyData) getFiles().get(idName));
						continue;
					}

					// (c) Prepare the python script that populates the tool state with remaining parameter settings required to invoke the tool
					task.prepareParamScript();
				}
			}

		} catch (IOException | JSONException | WorkflowStructureUnknownException e) {
			e.printStackTrace();
			e.printStackTrace();
			System.exit(-1);
		}

		getScheduler().addTasks(tasks.values());
	}

	/**
	 * A helper function for processing the Galaxy config file that specifies metadata for data tables along with the location of their loc files
	 * 
	 * @param file
	 *            the Galaxy data table config file
	 */
	private void processDataTables(File file) {
		try {
			System.out.println("Processing Galaxy data table config file " + file.getCanonicalPath());
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = builder.parse(file);
			NodeList tables = doc.getElementsByTagName("table");
			for (int i = 0; i < tables.getLength(); i++) {
				Element tableEl = (Element) tables.item(i);
				Element columnsEl = (Element) tableEl.getElementsByTagName("columns").item(0);
				Element fileEl = (Element) tableEl.getElementsByTagName("file").item(0);
				String name = tableEl.getAttribute("name");
				String comment_char = tableEl.getAttribute("comment_char");
				String[] columns = columnsEl.getFirstChild().getNodeValue().split(", ");
				if (!fileEl.hasAttribute("path"))
					continue;
				String path = fileEl.getAttribute("path");
				if (!path.startsWith("/"))
					path = galaxyPath + "/" + path;
				GalaxyDataTable galaxyDataTable = new GalaxyDataTable(name, comment_char, columns, path);
				processLocFile(new File(path), galaxyDataTable);
				galaxyDataTables.put(name, galaxyDataTable);
			}

		} catch (SAXException | IOException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * A helper function for processing the Galaxy config file that specifies the extensions and python script locations for Galaxy's data types
	 * 
	 * @param file
	 *            the Galaxy data type config file
	 */
	private void processDataTypes(File file) {
		try {
			System.out.println("Processing Galaxy data type config file " + file.getCanonicalPath());
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = builder.parse(file);
			NodeList datatypeNds = doc.getElementsByTagName("datatype");
			for (int i = 0; i < datatypeNds.getLength(); i++) {
				Element datatypeEl = (Element) datatypeNds.item(i);
				if (!datatypeEl.hasAttribute("extension") || !datatypeEl.hasAttribute("type") || datatypeEl.hasAttribute("subclass"))
					continue;
				String extension = datatypeEl.getAttribute("extension");
				String[] splitType = datatypeEl.getAttribute("type").split(":");
				galaxyDataTypes.put(extension, new GalaxyDataType(splitType[0], splitType[1], extension));
			}
		} catch (SAXException | IOException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * A (recursive) helper function for parsing the macros used by the XML files describing Galaxy's tools
	 * 
	 * @param macrosNd
	 *            an XML node that specifies a set of macros
	 * @param dir
	 *            the directory in which the currently processed macros are located
	 * @return processed macros accessible by their name
	 */
	private Map<String, String> processMacros(Node macrosNd, String dir) {
		Map<String, String> macrosByName = new HashMap<>();
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Element macrosEl = (Element) macrosNd;

			// (1) if additional macro files are to be imported, open the files and recursively invoke this method
			NodeList importNds = macrosEl.getElementsByTagName("import");
			for (int j = 0; j < importNds.getLength(); j++) {
				Element importEl = (Element) importNds.item(j);
				String importFileName = importEl.getChildNodes().item(0).getNodeValue().trim();
				File file = new File(dir, importFileName);
				Document doc = builder.parse(file);
				macrosByName.putAll(processMacros(doc.getDocumentElement(), dir));
			}

			// (2) parse all macros in this set
			NodeList macroNds = macrosEl.getElementsByTagName("macro");
			for (int j = 0; j < macroNds.getLength(); j++) {
				Element macroEl = (Element) macroNds.item(j);
				String name = macroEl.getAttribute("name");

				Transformer transformer = TransformerFactory.newInstance().newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				StreamResult result = new StreamResult(new StringWriter());
				DOMSource source = new DOMSource(macroEl);
				transformer.transform(source, result);
				String macro = result.getWriter().toString();
				macro = macro.substring(macro.indexOf('\n') + 1, macro.lastIndexOf('\n'));
				macrosByName.put(name, macro);
			}
		} catch (SAXException | IOException | TransformerException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return macrosByName;
	}

	/**
	 * A helper function for processing a Galaxy config file that lists the tools within a single library
	 * 
	 * @param file
	 *            the Galaxy tool library config file
	 * @param defaultPath
	 *            the directory in which pre-installed Galaxy tools are located
	 * @param dependencyDir
	 *            the directory in which the tool's dependencies are located
	 */
	private void processToolLibraries(File file, String defaultPath, String dependencyDir) {
		try {
			System.out.println("Processing Galaxy tool library config file " + file.getCanonicalPath());
			File galaxyPathFile = new File(galaxyPath);
			File dir = new File(galaxyPathFile, defaultPath);
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = builder.parse(file);
			Element toolboxEl = doc.getDocumentElement();
			if (toolboxEl.hasAttribute("tool_path")) {
				dir = new File(galaxyPathFile, toolboxEl.getAttribute("tool_path"));
			}

			NodeList tools = toolboxEl.getElementsByTagName("tool");
			for (int i = 0; i < tools.getLength(); i++) {
				// (1) parse a single XML tool file
				Element toolEl = (Element) tools.item(i);
				String toolFile = toolEl.getAttribute("file");
				GalaxyTool tool = parseToolFile(new File(dir, toolFile));

				// (2) go over the tool's dependencies and determine the environment-setting pre-script accordingly
				NodeList repositoryNameNds = toolEl.getElementsByTagName("repository_name");
				String repositoryName = repositoryNameNds.getLength() > 0 ? repositoryNameNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";
				NodeList ownerNds = toolEl.getElementsByTagName("repository_owner");
				String owner = ownerNds.getLength() > 0 ? ownerNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";
				NodeList revisionNds = toolEl.getElementsByTagName("installed_changeset_revision");
				String revision = revisionNds.getLength() > 0 ? revisionNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";

				if (repositoryName.length() > 0 && owner.length() > 0 && revision.length() > 0) {
					for (String requirementName : tool.getRequirements()) {
						File envFile = new File(galaxyPath + "/" + dependencyDir, requirementName + "/" + tool.getRequirementVersion(requirementName) + "/"
								+ owner + "/" + repositoryName + "/" + revision + "/env.sh");
						if (envFile.exists()) {
							try (BufferedReader br = new BufferedReader(new FileReader(envFile))) {
								String line;
								while ((line = br.readLine()) != null) {
									tool.addEnv(line);
								}
							}
						}
					}
				}

			}
		} catch (SAXException | IOException | ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
