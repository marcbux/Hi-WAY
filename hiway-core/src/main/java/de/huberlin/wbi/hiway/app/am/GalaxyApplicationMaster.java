package de.huberlin.wbi.hiway.app.am;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.app.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class GalaxyApplicationMaster extends HiWay {

	public static class GalaxyTool {
		private final String name;
		private String template;
		private Map<String, String> defaultParams;
		private Map<String, Set<GalaxyDataType>> inputTypes;
		private Map<String, Set<GalaxyDataType>> outputTypes;

		public GalaxyTool(String name) {
			this.name = name;
			defaultParams = new HashMap<>();
			inputTypes = new HashMap<>();
			outputTypes = new HashMap<>();
		}

		@Override
		public String toString() {
			return getName();
		}

		@Override
		public int hashCode() {
			return getName().hashCode();
		}
		public String getName() {
			return name;
		}

		public String getTemplate() {
			return template;
		}

		public void setTemplate(String template) {
			this.template = template;
		}

		public void addDefaultParam(String paramName, String defaultValue) {
			defaultParams.put(paramName, defaultValue);
		}

		public void addInputTypes(String inputName, String[] typeNames) {
			Set<GalaxyDataType> dataTypes = new HashSet<>();
			for (String typeName : typeNames)
				dataTypes.add(galaxyDataTypes.get(typeName));
			inputTypes.put(inputName, dataTypes);
		}

		public void addOutputTypes(String outputName, String[] typeNames) {
			Set<GalaxyDataType> dataTypes = new HashSet<>();
			for (String typeName : typeNames)
				dataTypes.add(galaxyDataTypes.get(typeName));
			outputTypes.put(outputName, dataTypes);
		}

		public void addOutputTypes(String outputName, Set<GalaxyDataType> dataTypes) {
			outputTypes.put(outputName, dataTypes);
		}

		public boolean hasDefaultParameter(String paramName) {
			return defaultParams.containsKey(paramName);
		}

		public boolean hasInputTypes(String inputName) {
			return inputTypes.containsKey(inputName);
		}

		public boolean hasOutputTypes(String outputName) {
			return outputTypes.containsKey(outputName);
		}

		public String getDefaultParameter(String paramName) {
			return defaultParams.get(paramName);
		}

		public Set<GalaxyDataType> getInputTypes(String inputName) {
			return inputTypes.get(inputName);
		}

		public Set<GalaxyDataType> getOutputTypes(String outputName) {
			return outputTypes.get(outputName);
		}

	}

	public static class GalaxyDataType {
		private final String name;
		private String extension;
		private GalaxyDataType parent;
		private Map<String, String> metadata;

		public GalaxyDataType(String name) {
			this.name = name;
			metadata = new HashMap<>();
		}

		public GalaxyDataType getParent() {
			return parent;
		}

		public boolean hasParent() {
			return parent != null;
		}

		public String getName() {
			return name;
		}

		public void setParent(GalaxyDataType parent) {
			this.parent = parent;
		}

		public String getExtension() {
			return extension;
		}

		public boolean hasExtension() {
			return extension != null;
		}

		public void setExtension(String extension) {
			this.extension = extension;
		}

		public void addMetadata(String name, String value) {
			metadata.put(name, value);
		}

		public boolean hasMetadata(String name) {
			if (metadata.containsKey(name)) {
				return true;
			} else if (parent != null) {
				return parent.hasMetadata(name);
			}
			return false;
		}

		public String getMetadata(String name) {
			if (metadata.containsKey(name)) {
				return metadata.get(name);
			} else if (parent != null) {
				return parent.getMetadata(name);
			}
			return null;
		}

	}

	public static class GalaxyTaskInstance extends TaskInstance {
//		private Map<String, Data> nameToData;
		private GalaxyTool galaxyTool;
		private StringBuilder pickleScript;

		public GalaxyTaskInstance(long id, String taskName, GalaxyTool galaxyTool) {
			// super(id, getRunId(), taskName, Math.abs(taskName.hashCode()), ForeignLambdaExpr.LANGID_BASH);
			super(id, UUID.randomUUID(), taskName, Math.abs(taskName.hashCode()), ForeignLambdaExpr.LANGID_BASH);
//			nameToData = new HashMap<>();
			this.galaxyTool = galaxyTool;
			pickleScript = new StringBuilder("import cPickle as pickle\n");
		}

//		public void addInputData(String name, Data data) {
//			nameToData.put(name, data);
//			super.addInputData(data);
//		}
//
//		public void addOutputData(String name, Data data) {
//			nameToData.put(name, data);
//			super.addOutputData(data);
//		}
//
//		public Data getDataByName(String name) {
//			return nameToData.get(name);
//		}
		
		public GalaxyTool getGalaxyTool() {
			return galaxyTool;
		}
		
		public void addParameter(String name, String value) {
			pickleScript.append(name + " = " + value + "\n");
			pickleScript.append("pickle.dump(" + name + ", open(\"pickle_" + id + ".p\", \"ab\"))\n");
		}
		
		public void buildPickleScript() {
			try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter("params_" + id + ".py"))) {
				scriptWriter.write(pickleScript.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void buildTemplate() {
			try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter("template_" + id + ".tmpl"))) {
				scriptWriter.write(getGalaxyTool().getTemplate());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static final Log log = LogFactory.getLog(GalaxyApplicationMaster.class);

	// public static void main(String[] args) {
	// HiWay.loop(new GalaxyApplicationMaster(), args);
	// }

	@Override
	public boolean init(String[] args) throws ParseException {
		super.init(args);
		galaxyTools = new HashMap<>();
//		pythonPath = hiWayConf.get(HiWayConfiguration.HIWAY_GALAXY_PYTHONPATH);
		if (!processDataTypeDir(new File(hiWayConf.get(HiWayConfiguration.HIWAY_GALAXY_DATATYPES))))
			return false;

		DocumentBuilder builder;
		try {
			builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			for (String toolDir : hiWayConf.get(HiWayConfiguration.HIWAY_GALAXY_TOOLS).split(",")) {
				if (!processToolDir(new File(toolDir.trim()), builder))
					return false;
			}
		} catch (ParserConfigurationException | FactoryConfigurationError e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void main(String[] args) {
		processDataTypeDir(new File("galaxy-galaxy-dist-5123ed7f1603/lib/galaxy/datatypes"));
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			galaxyTools = new HashMap<>();
			processToolDir(new File("galaxy-galaxy-dist-5123ed7f1603/tools"), builder);
		} catch (ParserConfigurationException | FactoryConfigurationError e) {
			e.printStackTrace();
		}
		parseWorkflow("galaxy101.ga");
	}

	private static boolean processDataTypeDir(File dir) {
		galaxyDataTypes = new HashMap<>();
		for (File file : dir.listFiles()) {
			if (file.getName().endsWith(".py")) {
				try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
					String line;
					String[] splitLine;
					GalaxyDataType dataType = null;
					while ((line = reader.readLine()) != null) {
						if (line.startsWith("class")) {
							line = line.replace("class", "").replaceAll(" ", "").replace(":", "").replace(")", "");
							splitLine = line.split("\\(");
							if (splitLine.length == 1)
								continue;
							String name = splitLine[0];
							dataType = addAndGetDataType(name);
							String parentNames = splitLine[1].substring(splitLine[1].lastIndexOf(".") + 1);
							String[] splitSplit = parentNames.split(",");
							GalaxyDataType parentType = addAndGetDataType(splitSplit[0]);
							dataType.setParent(parentType);
						} else if (line.startsWith("    file_ext = ")) {
							dataType.setExtension(line.replace("    file_ext = ", "").replaceAll("[\\\"']", ""));
						} else if (line.startsWith("    MetadataElement(")) {
							line = line.replace("MetadataElement(", "").replace(")", "").trim();
							splitLine = line.split(", ");
							String name = null, value = null;
							for (String split : splitLine) {
								String[] splitSplit = split.split("=");
								switch (splitSplit[0]) {
								case "name":
									name = splitSplit[1].replaceAll("[\\\"']", "");
									break;
								case "default":
									value = splitSplit[1].replaceAll("[\\\"']", "");
									break;
								case "no_value":
									if (value == null)
										value = splitSplit[1].replaceAll("[\\\"']", "");
								}
							}
							if ((name != null) && (value != null)) {
								dataType.addMetadata(name, value);
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
			}
		}

		Set<String> delete = new HashSet<>();
		for (String name : galaxyDataTypes.keySet())
			if (!isDataType(galaxyDataTypes.get(name)))
				delete.add(name);
		galaxyDataTypes.keySet().removeAll(delete);

		Map<String, GalaxyDataType> add = new HashMap<>();
		for (String name : galaxyDataTypes.keySet()) {
			GalaxyDataType dataType = galaxyDataTypes.get(name);
			add.put(name.toLowerCase(), dataType);
			if (dataType.hasExtension()) {
				String extension = dataType.getExtension();
				add.put(extension, dataType);
				add.put(extension.toLowerCase(), dataType);
			}
		}
		galaxyDataTypes.putAll(add);

//		for (GalaxyDataType dataType : galaxyDataTypes.values())
//			System.out.println(dataType.getExtension() + ": " + dataType.getName() + " --> " + dataType.getParent().getName());

		return true;
	}

	private static GalaxyDataType addAndGetDataType(String name) {
		if (!galaxyDataTypes.containsKey(name)) {
			galaxyDataTypes.put(name, new GalaxyDataType(name));
		}
		return galaxyDataTypes.get(name);
	}

	private static boolean isDataType(GalaxyDataType dataType) {
		if (dataType.getName().equals("Data"))
			return true;
		GalaxyDataType parent = dataType.getParent();
		if (parent != null)
			return isDataType(parent);
		return false;
	}

	private static boolean processToolDir(File dir, DocumentBuilder builder) {
		for (File file : dir.listFiles()) {
			if (file.isDirectory()) {
				processToolDir(file, builder);
			} else if (file.getName().endsWith(".xml")) {
				try {
					Document doc = builder.parse(file);
					Element rootEl = doc.getDocumentElement();
					if (rootEl.getNodeName() == "tool") {
						String toolName = rootEl.getAttribute("name");
						GalaxyTool tool = new GalaxyTool(toolName);

						Element commandEl = (Element) rootEl.getElementsByTagName("command").item(0);
						if (commandEl != null) {
							String command = commandEl.getChildNodes().item(0).getNodeValue();
							String interpreter = commandEl.getAttribute("interpreter");
							if (interpreter.length() > 0)
								command = interpreter + " " + command;
							tool.setTemplate(command);
						}

						Element inputsEl = (Element) rootEl.getElementsByTagName("inputs").item(0);
						if (inputsEl != null) {
							NodeList paramNds = inputsEl.getElementsByTagName("param");
							for (int i = 0; i < paramNds.getLength(); i++) {
								Element paramEl = (Element) paramNds.item(i);
								String type = paramEl.getAttribute("type");
								String paramName = paramEl.getAttribute("name");
								if (type.equals("data")) {
									String format = paramEl.getAttribute("format");
									String[] splitFormat = format.split(",");
									tool.addInputTypes(paramName, splitFormat);
								} else {
									String defaultValue = paramEl.getAttribute("value");
									if (defaultValue != null)
										tool.addDefaultParam(paramName, defaultValue);
								}
							}
						}

						Element outputsEl = (Element) rootEl.getElementsByTagName("outputs").item(0);
						if (outputsEl != null) {
							NodeList dataNds = outputsEl.getElementsByTagName("data");
							for (int i = 0; i < dataNds.getLength(); i++) {
								Element dataEl = (Element) dataNds.item(i);
								String outputName = dataEl.getAttribute("name");
								String format = dataEl.getAttribute("format");
								String metadata_source = dataEl.getAttribute("metadata_source");
								if (format.equals("input")) {
									tool.addOutputTypes(outputName, tool.getInputTypes(metadata_source));
								} else {
									String[] splitFormat = format.split(",");
									tool.addOutputTypes(outputName, splitFormat);
								}
							}
						}

						if (tool.getTemplate() != null) {
//							System.out.println(file + ":" + tool.getName() /* + " " + tool.getCommand() */);
							galaxyTools.put(tool.getName(), tool);
						}

					}
				} catch (SAXException | IOException e) {
					e.printStackTrace();
					return false;
				}
			}
		}
		return true;
	}

//	private String pythonPath;
	// private static String path;
	private static Map<String, GalaxyTool> galaxyTools;
	private static Map<String, GalaxyDataType> galaxyDataTypes;

	public GalaxyApplicationMaster() {
		super();
		// path = "$PATH";
	}

	@Override
	public void parseWorkflow() {
	}

	public static void parseWorkflow(String _fileName) {
		Map<String, Data> files = new HashMap<>();

		// log.info("Parsing Galaxy workflow " + workflowFile);
		Map<Long, GalaxyTaskInstance> tasks = new HashMap<>();
		// try (BufferedReader reader = new BufferedReader(new FileReader(workflowFile.getLocalPath()))) {
		try (BufferedReader reader = new BufferedReader(new FileReader(_fileName))) {
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
			JSONObject workflow = new JSONObject(sb.toString());
			JSONObject steps = workflow.getJSONObject("steps");

			// (1) Parse Nodes
			for (int i = 0; i < steps.length(); i++) {
				JSONObject step = steps.getJSONObject(Integer.toString(i));
				long id = step.getLong("id");
				String type = step.getString("type");

				if (type.equals("data_input")) {
					JSONArray inputs = step.getJSONArray("inputs");
					for (int j = 0; j < inputs.length(); j++) {
						JSONObject input = inputs.getJSONObject(j);
						String name = input.getString("name");
						String fileName = name;
						String idName = id + "_output";
						Data data = new Data(fileName);
						data.setInput(true);
						files.put(idName, data);
					}
				} else if (type.equals("tool")) {
					String name = step.getString("name");
					GalaxyTool tool = galaxyTools.get(name);
					if (tool == null) {
						log.error("Tool " + name + " could not be located in local Galaxy installation.");
//						onError(new RuntimeException());
					}

					GalaxyTaskInstance task = new GalaxyTaskInstance(id, name, tool);
					tasks.put(id, task);

					Map<String, String> renameOutputs = new HashMap<>();
					Set<String> hideOutputs = new HashSet<>();
					if (step.has("post_job_actions")) {
						JSONObject post_job_actions = step.getJSONObject("post_job_actions");
						for (Iterator<?> it = post_job_actions.keys(); it.hasNext();) {
							JSONObject post_job_action = post_job_actions.getJSONObject((String) it.next());
							String action_type = post_job_action.getString("action_type");
							if (action_type.equals("RenameDatasetAction")) {
								String output_name = post_job_action.getString("output_name");
								JSONObject action_arguments = post_job_action.getJSONObject("action_arguments");
								String newname = action_arguments.getString("newname");
								renameOutputs.put(output_name, newname);
							} else if (action_type.equals("HideDatasetAction")) {
								String output_name = post_job_action.getString("output_name");
								hideOutputs.add(output_name);
							}
						}
					}

					JSONArray outputs = step.getJSONArray("outputs");
					List<String> outputFiles = new LinkedList<>();
					StringBuilder outputParameter = new StringBuilder("{");
					for (int j = 0; j < outputs.length(); j++) {
						JSONObject output = outputs.getJSONObject(j);
						String outputName = output.getString("name");
						Set<GalaxyDataType> outputTypes = tool.getOutputTypes(outputName);
						String fileName = id + "_" + outputName;
						for (GalaxyDataType outputType : outputTypes)
							if (outputType.hasExtension())
								fileName = fileName + "." + outputType.getExtension();
						if (renameOutputs.containsKey(outputName))
							fileName = renameOutputs.get(outputName);
						String idName = id + "_" + outputName;
						Data data = new Data(fileName);
						if (!hideOutputs.contains(outputName)) {
							data.setOutput(true);
						}
						files.put(idName, data);
						task.addOutputData(data);
						outputParameter.append("\"" + outputName + "\": \"" + data.getLocalPath() + "\"");
						if (j+1 < outputs.length())
							outputParameter.append(", ");
						outputFiles.add(fileName);
					}
					outputParameter.append("}");
					task.addParameter("output", outputParameter.toString());

					task.getReport().add(
							new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(), null,
									JsonReportEntry.KEY_INVOC_OUTPUT, new JSONObject().put("output", outputFiles)));
				}
			}

			// (2) Parse Edges and determine command
			for (int i = 0; i < steps.length(); i++) {
				JSONObject step = steps.getJSONObject(Integer.toString(i));
				long id = step.getLong("id");
				String type = step.getString("type");

				if (type.equals("tool")) {
					GalaxyTaskInstance task = tasks.get(id);
					
					StringBuilder inputParameter = new StringBuilder("{");
					JSONObject input_connections = step.getJSONObject("input_connections");
					for (Iterator<?> it = input_connections.keys(); it.hasNext();) {
						String input_connection_key = (String) it.next();
						JSONObject input_connection = input_connections.getJSONObject(input_connection_key);
						long parentId = input_connection.getLong("id");
						String idName = parentId + "_" + input_connection.getString("output_name");
						TaskInstance parentTask = tasks.get(parentId);
						if (parentTask != null) {
							task.addParentTask(parentTask);
							parentTask.addChildTask(task);
						}
						task.addInputData(files.get(idName));
						inputParameter.append("\"" + input_connection_key + "\": \"" + files.get(idName) + "\"");
						if (it.hasNext())
							inputParameter.append(", ");
						continue;
					}
					inputParameter.append("}");
					task.addParameter("input", inputParameter.toString());
					
					// (a) Build pickle python script
					task.addParameter("tool_state", step.getString("tool_state").replaceAll("null", "\"null\"").replaceAll("\\\\", "").replaceAll("\"\"", "\"").replaceAll("\"\\{", "\\{").replaceAll("\\}\"", "\\}").replaceAll("\"\\[", "\\[").replaceAll("\\]\"", "\\]").replaceAll("\\{\"__class__\":[^\"]*\"UnvalidatedValue\",[^\"]*\"value\":[^\"](\"[^\"]*\")\\}", "$1"));
					task.buildPickleScript();
					task.buildTemplate();
					
					
					
					/*
					
					
					
					
					JSONObject tool_state = new JSONObject(tool_state_s);
					Map<String, String> params = new HashMap<>();
					for (Iterator<?> it = tool_state.keys(); it.hasNext();) {
						String key = (String) it.next();
						String value = tool_state.getString(key).replaceAll("\\\"", "");
						params.put(key, value);
					}
					
					
					
					String command = tool.getTemplate();
					System.out.println(command);
					Pattern paramPattern = Pattern.compile("\\$[^ ,]*");
					Matcher paramMatcher = paramPattern.matcher(command);
					while (paramMatcher.find()) {
						String match = paramMatcher.group();
						String matchRegEx = match.replace("$", "\\$").replace("{", "\\{").replace("}", "\\}");
						System.out.print("\t" + match);
						String paramName = match.replace("$", "").replace("{", "").replace("}", "");

						if (tool.hasInputTypes(paramName)) {
							command = command.replaceAll(matchRegEx, task.getDataByName(paramName).getLocalPath());
							System.out.println(" <-- " + task.getDataByName(paramName).getLocalPath());
							continue;
						}

						if (tool.hasOutputTypes(paramName)) {
							command = command.replaceAll(matchRegEx, task.getDataByName(paramName).getLocalPath());
							System.out.println(" <-- " + task.getDataByName(paramName).getLocalPath());
							continue;
						}
						
						if (paramName.contains(".metadata.")) {
							String[] splitParamName = paramName.split("\\.");
							Set<GalaxyDataType> dataTypes = tool.hasInputTypes(splitParamName[0]) ? tool.getInputTypes(splitParamName[0]) : tool.getOutputTypes(splitParamName[0]);
							for (GalaxyDataType dataType : dataTypes) {
								if (dataType.hasMetadata(splitParamName[2])) {
									command = command.replaceAll(matchRegEx, dataType.getMetadata(splitParamName[2]));
									System.out.println(" <-- " + dataType.getMetadata(splitParamName[2]));
									break;
								}
							}
							continue;
						}

						if (params.containsKey(paramName)) {
							command = command.replace(match, params.get(paramName));
							System.out.println(" <-- " + params.get(paramName));
							continue;
						}

						if (tool.hasDefaultParameter(paramName)) {
							command = command.replace(match, tool.getDefaultParameter(paramName));
							System.out.println(" <-- " + tool.getDefaultParameter(paramName));
							continue;
						}
					}

					System.out.println(command);
					task.setCommand(command);
					task.getReport().add(
							new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), task.getId(), null,
									JsonReportEntry.KEY_INVOC_SCRIPT, task.getCommand()));
									
									*/
				}
			}

		} catch (IOException | JSONException | WorkflowStructureUnknownException e) {
			e.printStackTrace();
			// onError(e);
		}

		// scheduler.addTasks(tasks.values());
	}
}
