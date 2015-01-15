package de.huberlin.wbi.hiway.app.am;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
import java.util.UUID;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.ForeignLambdaExpr;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.app.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class GalaxyApplicationMaster extends HiWay {

	public static final String galaxyPath = "D:\\Documents\\Workspace2\\hiway\\hiway-core\\galaxy-galaxy-dist-5123ed7f1603";
	public static final String toolShedPath = "D:\\Documents\\Workspace2\\hiway\\hiway-core\\shed_tools";
	// public static final String workflowPath = "galaxy101.ga";
	public static final String workflowPath = "RNAseq.ga";

	public static class GalaxyDataTable {
		private final String name;
		private final String comment_char;
		private final String[] columns;
		private Map<String, Map<String, String>> contentByValue;
		private final String path;

		public GalaxyDataTable(String name, String comment_char, String[] columns, String path) {
			this.name = name;
			this.comment_char = comment_char;
			this.columns = columns;
			this.path = path;
			contentByValue = new HashMap<>();
		}

		public String[] getColumns() {
			return columns;
		}

		public String getComment_char() {
			return comment_char;
		}

		public String getName() {
			return name;
		}

		public String getPath() {
			return path;
		}

		public void addContent(String[] content) {
			Map<String, String> newContent = new HashMap<>();
			for (int i = 0; i < columns.length; i++) {
				newContent.put(columns[i], content[i]);
				if (columns[i].equals("value"))
					contentByValue.put(content[i], newContent);
			}
		}

		public JSONObject getContent(String value) {
			JSONObject inner = new JSONObject(contentByValue.get(value));
			JSONObject outer = new JSONObject();
			try {
				outer.put("fields", inner);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return outer;
		}

		public Set<String> getValues() {
			return contentByValue.keySet();
		}
	}

	public static class GalaxyData extends Data {
		String extension;
		GalaxyDataType dataType;

		public GalaxyData(String path) {
			super(path);
		}

		public void setDataType(GalaxyDataType dataType) {
			this.dataType = dataType;
		}

		public GalaxyDataType getDataType() {
			return dataType;
		}

		public boolean hasDataType() {
			return dataType != null;
		}
	}

	public static abstract class GalaxyParam {
		protected final String name;

		public GalaxyParam(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public abstract Set<GalaxyParamValue> getParamValues();
	}

	public static class GalaxyConditional extends GalaxyParam {
		GalaxyParamValue condition;
		Map<String, Set<GalaxyParam>> conditionalParams;

		public GalaxyConditional(String name) {
			super(name);
			conditionalParams = new HashMap<>();
		}

		public void setCondition(GalaxyParamValue condition) {
			this.condition = condition;
		}

		public void setConditionalParams(String conditionValue, Set<GalaxyParam> params) {
			conditionalParams.put(conditionValue, params);
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
	}

	public static class GalaxyRepeat extends GalaxyParam {
		Set<GalaxyParam> params;

		public GalaxyRepeat(String name) {
			super(name);
		}

		public void setParams(Set<GalaxyParam> params) {
			this.params = params;
		}

		@Override
		public Set<GalaxyParamValue> getParamValues() {
			Set<GalaxyParamValue> paramValues = new HashSet<>();
			for (GalaxyParam param : params)
				paramValues.addAll(param.getParamValues());
			return paramValues;
		}
	}

	public static class GalaxyParamValue extends GalaxyParam {
		private String defaultValue;
		private Map<Object, Object> mappings;
		private String dataType;
		private String from_work_dir;

		public GalaxyParamValue(String name) {
			super(name);
			mappings = new HashMap<>();
		}

		public void addMapping(Object from, Object to) {
			mappings.put(from, to);
		}

		public boolean hasMapping(Object from) {
			Object to = mappings.get(from);
			return to != null;
		}

		public Object getMapping(Object from) {
			return mappings.get(from);
		}

		public void setDefaultValue(String value) {
			this.defaultValue = value;
			addMapping("", value);
			// addMapping("null", value);
			// addMapping(null, value);
		}

		public boolean hasDefaultValue() {
			return defaultValue != null && defaultValue.length() > 0 && !defaultValue.equals("");
		}

		public String getDefaultValue() {
			return defaultValue;
		}

		// public void addDataTypes(String[] typeNames) {
		// for (String typeName : typeNames)
		// dataTypes.add(galaxyDataTypes.get(typeName));
		// }

		// public void addDataTypes(Set<GalaxyDataType> dataTypes) {
		// this.dataTypes.addAll(dataTypes);
		// }

		public String getDataType() {
			return dataType;
		}

		public void setDataType(String dataType) {
			this.dataType = dataType;
		}

		@Override
		public Set<GalaxyParamValue> getParamValues() {
			Set<GalaxyParamValue> paramValues = new HashSet<>();
			paramValues.add(this);
			return paramValues;
		}

		// public Set<GalaxyDataType> getDataTypes() {
		// return dataTypes;
		// }

		public String getFrom_work_dir() {
			return from_work_dir;
		}

		public void setFrom_work_dir(String from_work_dir) {
			this.from_work_dir = from_work_dir;
		}

		public boolean hasFrom_work_dir() {
			return from_work_dir != null && from_work_dir.length() > 0;
		}
	}

	public static class GalaxyTool {
		private final String id;
		private final String version;
		private String template;
		private Set<GalaxyParam> params;
		private String env;
		private Map<String, String> requirements;

		// private final String changeset_revision;
		// private final String owner;

		public GalaxyTool(String id, String version, String dir) {
			this.id = id;
			this.version = version;
			params = new HashSet<>();
			this.env = "PATH=" + dir + ":$PATH; export PATH\n" + "PYTHONPATH=" + galaxyPath + "/lib:$PYTHONPATH; export PYTHONPATH\n";
			requirements = new HashMap<>();
		}

		public void addRequirement(String name, String version) {
			requirements.put(name, version);
		}

		public Set<String> getRequirements() {
			return requirements.keySet();
		}

		public String getRequirementVersion(String name) {
			return requirements.get(name);
		}

		public void addEnv(String env) {
			this.env = this.env + (env.endsWith("\n") ? env : env + "\n");
		}

		public GalaxyParamValue getFirstMatchingParamByName(String name) {
			for (GalaxyParam param : params)
				for (GalaxyParamValue paramValue : param.getParamValues())
					if (paramValue.getName().equals(name))
						return paramValue;
			return null;
		}

		public String getEnv() {
			return env;
		}

		// public Set<GalaxyParamValue> getDataParams() {
		// Set<GalaxyParamValue> dataParams = new HashSet<>();
		// for (GalaxyParam param : params)
		// for (GalaxyParamValue paramValue : param.getParamValues())
		// if (paramValue.getDataTypes().size() > 0)
		// dataParams.add(paramValue);
		// return dataParams;
		// }

		@Override
		public String toString() {
			return getName();
		}

		@Override
		public int hashCode() {
			return getName().hashCode();
		}

		private String getName() {
			return getId() + "/" + getVersion();
		}

		public String getId() {
			return id;
		}

		public String getVersion() {
			return version;
		}

		public String getTemplate() {
			return template;
		}

		public void setTemplate(String template) {
			this.template = template.endsWith("\n") ? template : template + "\n";
		}

		private void mapParams(JSONObject jo) throws JSONException {
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
			// ??? (1) go through tool parameters and add default values to the toolstate

			// (2) go through toolstate and map parameters (if a mapping exists)
			mapParams(toolState);

			// (3) append Metadata
			// for (GalaxyParamValue dataParam : getDataParams()) {
			// Map<String, String> metadata = new HashMap<>();
			// for (GalaxyDataType type : dataParam.getDataTypes()) {
			// metadata.putAll(type.getMetadata());
			// String extension = type.getExtension();
			// if (extension != null && extension.length() > 0)
			// toolState.put(dataParam.getName() + "_extension", extension);
			// }
			// toolState.put(dataParam.getName() + "_metadata", metadata);
			// }

			// (4) add obligatory parameters
			toolState.put("__new_file_path__", ".");
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
					// String temp = template;
					// p = Pattern.compile("(\\$[^\\s]*)" + name + "([\\}'\"\\s]+)");
					// m = p.matcher(template);
					// while (m.find()) {
					// if (!template.substring(m.end()).startsWith(" in ")) {
					//
					// }
					// }

					String fileName = data.getName();
					JSONObject fileJo = new JSONObject();
					fileJo.putOpt("path", fileName);
					fileJo.putOpt("name", fileName.split("\\.(?=[^\\.]+$)")[0]);
					fileJo.putOpt("files_path", data.getLocalDirectory());

					if (data.hasDataType()) {
						GalaxyDataType dataType = data.getDataType();
						if (dataType.hasExtension()) {
							String fileExt = dataType.getExtension();
							fileJo.putOpt("extension", fileExt);
							fileJo.putOpt("ext", fileExt);
						}
					}

					if (data.hasDataType())
						fileJo.putOpt("metadata", new JSONObject(data.getDataType().getMetadata()));

					jo.putOpt(name, fileJo);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

		public void setParams(Set<GalaxyParam> params) {
			this.params = params;
		}

		public void addParam(String name, GalaxyParam param) {
			params.add(param);
		}

		//

		//
		// public Set<GalaxyDataType> getDataTypes(String inputName) {
		// return dataTypes.get(inputName);
		// }
		//
		// public Map<String, Set<GalaxyDataType>> getDataTypes() {
		// return dataTypes;
		// }

		// public boolean hasParam(String name) {
		// return params.containsKey(name);
		// }
		//
		// public GalaxyParam getParam(String name) {
		// return params.get(name);
		// }
		//
		// public Map<String, GalaxyParam> getParams() {
		// return params;
		// }
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

		public Map<String, String> getMetadata() {
			Map<String, String> allMetadata = new HashMap<>();
			if (parent != null) {
				allMetadata.putAll(parent.getMetadata());
			}
			allMetadata.putAll(metadata);
			return allMetadata;
		}

	}

	public static class GalaxyTaskInstance extends TaskInstance {
		private GalaxyTool galaxyTool;
		private StringBuilder pickleScript;
		private JSONObject toolState;

		public GalaxyTaskInstance(long id, String taskName, GalaxyTool galaxyTool) {
			super(id, UUID.randomUUID(), taskName, Math.abs(taskName.hashCode()), ForeignLambdaExpr.LANGID_BASH);
			this.galaxyTool = galaxyTool;
			pickleScript = new StringBuilder("import cPickle as pickle\ntool_state = ");
			toolState = new JSONObject();
			this.postScript = "";
		}

		private String postScript;

		public String getPostScript() {
			return postScript;
		}

		public GalaxyTool getGalaxyTool() {
			return galaxyTool;
		}

		public void addToPostScript(String post) {
			postScript = postScript + (post.endsWith("\n") ? post : post + "\n");
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
			toolState_json = toolState_json.replaceAll("\\{\"__class__\":[^\"]*\"UnvalidatedValue\",[^\"]*\"value\":[^\"](\"[^\"]*\")\\}", "$1");
			// replace "null" with ""
			toolState_json = toolState_json.replaceAll("\"null\"", "\"\"");
			try {
				this.toolState = new JSONObject(toolState_json);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

		public void addFile(String name, GalaxyData data) {
			galaxyTool.addFile(name, data, toolState);
		}

		public void buildPickleScript() throws JSONException {
			galaxyTool.populateToolState(toolState);
			pickleScript.append(toolState.toString());
			pickleScript.append("\npickle.dump(tool_state, open(\"" + workflowPath + "." + id + ".pickle.p\", \"wb\"))\n");
			try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(workflowPath + "." + id + ".params.py"))) {
				scriptWriter.write(pickleScript.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void buildTemplate() {
			String preScript = galaxyTool.getEnv();
			String template = galaxyTool.getTemplate();
			String postScript = getPostScript();

			// ???
			preScript = preScript.replaceAll(toolShedPath.replaceAll("\\\\", "\\\\\\\\"), "/home/hiway/software/shed_tools");
			preScript = preScript.replaceAll(galaxyPath.replaceAll("\\\\", "\\\\\\\\"), "/home/hiway/software/galaxy");
			preScript = preScript.replaceAll("\\\\", "/");

			try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(workflowPath + "." + id + ".pre.sh"))) {
				scriptWriter.write(preScript);
			} catch (IOException e) {
				e.printStackTrace();
			}

			try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(workflowPath + "." + id + ".template.tmpl"))) {
				scriptWriter.write(template);
			} catch (IOException e) {
				e.printStackTrace();
			}

			try (BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(workflowPath + "." + id + ".post.sh"))) {
				scriptWriter.write(postScript);
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
		// pythonPath = hiWayConf.get(HiWayConfiguration.HIWAY_GALAXY_PYTHONPATH);
		if (!processDataTypeDir(new File(hiWayConf.get(HiWayConfiguration.HIWAY_GALAXY_DATATYPES))))
			return false;

		DocumentBuilder builder;
		try {
			builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			for (String toolDir : hiWayConf.get(HiWayConfiguration.HIWAY_GALAXY_TOOLS).split(",")) {
				// if (!processToolDir(new File(toolDir.trim()), builder))
				// return false;
				log.info(builder.toString() + toolDir.toString());
			}
		} catch (ParserConfigurationException | FactoryConfigurationError e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void main(String[] args) {
		galaxyDataTypes = new HashMap<>();
		processDataTypeDir(new File(galaxyPath + "/lib/galaxy/datatypes"));

		String tool_data_table_config_path = "config/tool_data_table_conf.xml.sample";
		String shed_tool_data_table_config = "config/shed_tool_data_table_conf.xml.sample";
		String tool_dependency_dir = "dependencies";
		String tool_path = "tools";
		String tool_config_file = "config/tool_conf.xml.sample";
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
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] tool_config_files = tool_config_file.split(",");

		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

			galaxyDataTables = new HashMap<>();
			processDataTables(new File(galaxyPath + "/" + tool_data_table_config_path), builder);
			processDataTables(new File(galaxyPath + "/" + shed_tool_data_table_config), builder);

			galaxyTools = new HashMap<>();
			// parseToolDir(new File(galaxyPath + "/tools"), builder);
			// parseToolDir(new File(toolShedPath), builder);
			for (String config_file : tool_config_files) {
				processToolLibraries(new File(galaxyPath + "/" + config_file.trim()), builder, tool_path, tool_dependency_dir);
			}

			// processTools(builder);
		} catch (ParserConfigurationException | FactoryConfigurationError e) {
			e.printStackTrace();
		}
		parseWorkflow(workflowPath);
	}

	private static boolean processDataTables(File file, DocumentBuilder builder) {

		try {
			Document doc = builder.parse(file);
			NodeList tables = doc.getElementsByTagName("table");
			for (int i = 0; i < tables.getLength(); i++) {
				Element tableEl = (Element) tables.item(i);
				Element columnsEl = (Element) tableEl.getElementsByTagName("columns").item(0);
				Element fileEl = (Element) tableEl.getElementsByTagName("file").item(0);
				String name = tableEl.getAttribute("name");
				String comment_char = tableEl.getAttribute("comment_char");
				String[] columns = columnsEl.getFirstChild().getNodeValue().split(", ");
				String path = fileEl.getAttribute("path");
				// ???
				// if (!path.startsWith("/")) path = galaxyPath + "/" + path;
				if (!path.startsWith("/"))
					path = galaxyPath + "\\" + path;
				GalaxyDataTable galaxyDataTable = new GalaxyDataTable(name, comment_char, columns, path);
				processLocFile(new File(path), galaxyDataTable);
				galaxyDataTables.put(name, galaxyDataTable);
			}

		} catch (SAXException | IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	private static void processLocFile(File file, GalaxyDataTable galaxyDataTable) {
		if (!file.exists())
			return;
		try (BufferedReader locBr = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = locBr.readLine()) != null) {
				if (line.startsWith(galaxyDataTable.getComment_char()))
					continue;
				String[] content = line.split("\t");
				galaxyDataTable.addContent(content);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static boolean processToolLibraries(File file, DocumentBuilder builder, String defaultPath, String dependencyDir) {
		try {
			File galaxyPathFile = new File(galaxyPath);
			File dir = new File(galaxyPathFile, defaultPath);
			Document doc = builder.parse(file);
			Element toolboxEl = doc.getDocumentElement();
			if (toolboxEl.hasAttribute("tool_path")) {
				dir = new File(galaxyPathFile, toolboxEl.getAttribute("tool_path"));
			}

			NodeList tools = toolboxEl.getElementsByTagName("tool");
			for (int i = 0; i < tools.getLength(); i++) {
				Element toolEl = (Element) tools.item(i);
				String toolFile = toolEl.getAttribute("file");
				GalaxyTool tool = parseToolFile(new File(dir, toolFile), builder);

				// else if (rootEl.getNodeName() == "tool_dependency") {
				// NodeList packageNds = (NodeList) rootEl.getElementsByTagName("package");
				// for (int i = 0; i < packageNds.getLength(); i++) {
				// Element packageEl = (Element) packageNds.item(i);
				// String packageName = packageEl.getAttribute("name");
				// String version = packageEl.getAttribute("version");
				//
				// NodeList repositoryNds = (NodeList) packageEl.getElementsByTagName("repository");
				// for (int j = 0; j < repositoryNds.getLength(); j++) {
				// Element repositoryEl = (Element) repositoryNds.item(j);
				// String changeset_revision = repositoryEl.getAttribute("changeset_revision");
				// String repositoryName = repositoryEl.getAttribute("name");
				// String owner = repositoryEl.getAttribute("owner");
				//
				// File envFile = new File(galaxyPath + "/dependencies/" + packageName + "/" + version + "/" + owner + "/" + repositoryName + "/"
				// + changeset_revision + "/env.sh");
				// if (envFile.exists()) {
				// try (BufferedReader br = new BufferedReader(new FileReader(envFile))) {
				// String line;
				// while ((line = br.readLine()) != null) {
				// env.append(line).append("\n");
				// }
				// }
				// }
				//
				// }
				// }
				// }

				// NodeList repositoryNds = toolEl.getElementsByTagName("tool_shed");
				// String repository = repositoryNds.getLength() > 0 ? repositoryNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";
				NodeList repositoryNameNds = toolEl.getElementsByTagName("repository_name");
				String repositoryName = repositoryNameNds.getLength() > 0 ? repositoryNameNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";
				NodeList ownerNds = toolEl.getElementsByTagName("repository_owner");
				String owner = ownerNds.getLength() > 0 ? ownerNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";
				NodeList revisionNds = toolEl.getElementsByTagName("installed_changeset_revision");
				String revision = revisionNds.getLength() > 0 ? revisionNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";
				// NodeList versionNds = toolEl.getElementsByTagName("version");
				// String version = versionNds.getLength() > 0 ? versionNds.item(0).getChildNodes().item(0).getNodeValue().trim() : "";

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
		} catch (SAXException | IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	private static boolean processDataTypeDir(File dir) {
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
							String extension = line.replace("    file_ext = ", "").replaceAll("[\\\"']", "");
							dataType.setExtension(extension);
							galaxyDataTypes.put(extension, dataType);
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

		// for (GalaxyDataType dataType : galaxyDataTypes.values())
		// System.out.println(dataType.getExtension() + ": " + dataType.getName() + " --> " + dataType.getParent().getName());

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

	private static Set<GalaxyParam> getParams(Element el) throws XPathExpressionException {
		Set<GalaxyParam> params = new HashSet<>();
		XPath xpath = XPathFactory.newInstance().newXPath();
		NodeList paramNds = (NodeList) xpath.evaluate("param", el, XPathConstants.NODESET);
		NodeList conditionalNds = (NodeList) xpath.evaluate("conditional", el, XPathConstants.NODESET);
		NodeList repeatNds = (NodeList) xpath.evaluate("repeat", el, XPathConstants.NODESET);

		for (int i = 0; i < paramNds.getLength(); i++) {
			Element paramEl = (Element) paramNds.item(i);
			String name = paramEl.getAttribute("name");
			GalaxyParamValue param = new GalaxyParamValue(name);
			params.add(param);

			String type = paramEl.getAttribute("type");
			switch (type) {
			// case "data":
			// String format = paramEl.getAttribute("format");
			// String[] splitFormat = format.split(",");
			// param.addDataTypes(splitFormat);
			// break;
			case "boolean":
				String trueValue = paramEl.getAttribute("truevalue");
				param.addMapping("True", trueValue);
				String falseValue = paramEl.getAttribute("falsevalue");
				param.addMapping("False", falseValue);
			case "select":
				param.addMapping("", "None");
				// param.setDefaultValue("None");
			default:
				String defaultValue = paramEl.getAttribute("value");
				if (defaultValue != null && defaultValue.length() > 0)
					param.setDefaultValue(defaultValue);
			}

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
				conditional.setConditionalParams(conditionValue, getParams(whenEl));
			}

			params.add(conditional);
		}

		for (int i = 0; i < repeatNds.getLength(); i++) {
			Element repeatEl = (Element) repeatNds.item(i);
			String name = repeatEl.getAttribute("name");
			GalaxyRepeat repeat = new GalaxyRepeat(name);
			params.add(repeat);

			repeat.setParams(getParams(repeatEl));
		}

		return params;
	}

	// private static Map<String, String> toolDescriptionToDir = new HashMap<>();
	// private static Map<String, String> dirToEnv = new HashMap<>();
	private static Map<String, String> macrosByName = new HashMap<>();

	private static GalaxyTool parseToolFile(File file, DocumentBuilder builder) {
		try {
			String path = file.getCanonicalPath();
			// ???
			String dir = path.substring(0, path.lastIndexOf("\\"));
			// StringBuilder env = new StringBuilder();
			Document doc = builder.parse(file);
			Element rootEl = doc.getDocumentElement();
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			StreamResult result = new StreamResult(new StringWriter());
			DOMSource source = new DOMSource(rootEl);
			transformer.transform(source, result);
			String toolDescription = result.getWriter().toString();

			NodeList macrosNds = (NodeList) rootEl.getElementsByTagName("macros");
			for (int i = 0; i < macrosNds.getLength(); i++) {
				Node macrosNd = macrosNds.item(i);
				processMacros(macrosNd, builder, dir);
			}

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
			GalaxyTool tool = new GalaxyTool(id, version, dir);
			
			NodeList requirementNds = rootEl.getElementsByTagName("requirement");
			for (int i = 0; i < requirementNds.getLength(); i++) {
				Element requirementEl = (Element) requirementNds.item(i);
				String requirementName = requirementEl.getChildNodes().item(0).getNodeValue().trim();
				String requirementVersion = requirementEl.getAttribute("version");
				tool.addRequirement(requirementName, requirementVersion);
			}

			Element commandEl = (Element) rootEl.getElementsByTagName("command").item(0);
			if (commandEl != null) {
				String command = commandEl.getChildNodes().item(0).getNodeValue().trim();
				String script = command.split(" ")[0];
				String interpreter = commandEl.getAttribute("interpreter");
				if (interpreter.length() > 0) {
					// ???
					dir = dir.replaceAll(toolShedPath.replaceAll("\\\\", "\\\\\\\\"), "/home/hiway/software/shed_tools");
					dir = dir.replaceAll(galaxyPath.replaceAll("\\\\", "\\\\\\\\"), "/home/hiway/software/galaxy");
					dir = dir.replaceAll("\\\\", "/");

					command = command.replace(script, dir + "/" + script);
					command = interpreter + " " + command;
				}
				// command = command.replaceAll("\\.metadata\\.", "_metadata.");
				// command = command.replaceAll("\\.extension", "_extension");
				command = command.replaceAll("\\.value", "");
				command = command.replaceAll("\\.dataset", "");
				// command = command.replaceAll("\\.fields\\.path", "");
				tool.setTemplate(command);
			}

			Element inputsEl = (Element) rootEl.getElementsByTagName("inputs").item(0);
			if (inputsEl != null)
				tool.setParams(getParams(inputsEl));

			Element outputsEl = (Element) rootEl.getElementsByTagName("outputs").item(0);
			if (outputsEl != null) {
				NodeList dataNds = outputsEl.getElementsByTagName("data");
				for (int i = 0; i < dataNds.getLength(); i++) {
					Element dataEl = (Element) dataNds.item(i);
					String name = dataEl.getAttribute("name");
					GalaxyParamValue param = new GalaxyParamValue(name);
					tool.addParam(name, param);

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

			if (tool.getTemplate() != null) {
				Map<String, GalaxyTool> toolMap = addAndGetToolMap(id);
				toolMap.put(version, tool);
			}

			return tool;

		} catch (SAXException | IOException | TransformerException | XPathExpressionException e) {
			return null;
		}
	}

	private static boolean processMacros(Node macrosNd, DocumentBuilder builder, String dir) {
		try {
			Element macrosEl = (Element) macrosNd;
			NodeList importNds = macrosEl.getElementsByTagName("import");
			for (int j = 0; j < importNds.getLength(); j++) {
				Element importEl = (Element) importNds.item(j);
				String importFileName = importEl.getChildNodes().item(0).getNodeValue().trim();
				File file = new File(dir, importFileName);
				Document doc = builder.parse(file);
				processMacros(doc.getDocumentElement(), builder, dir);
			}

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
				macro = macro.substring(macro.indexOf('\n') + 1, macro.lastIndexOf('\n') - 1);
				macrosByName.put(name, macro);
			}
		} catch (SAXException | IOException | TransformerException e) {
			e.printStackTrace();
		}
		return true;
	}

	// private static boolean processTools(DocumentBuilder builder) {
	// for (String toolDescription : toolDescriptionToDir.keySet()) {
	// try {
	//
	// } catch (SAXException | IOException | XPathExpressionException e) {
	// e.printStackTrace();
	// return false;
	// }
	// }
	// return true;
	// }

	private static Map<String, GalaxyTool> addAndGetToolMap(String id) {
		if (!galaxyTools.containsKey(id)) {
			Map<String, GalaxyTool> toolMap = new HashMap<>();
			galaxyTools.put(id, toolMap);
		}
		return galaxyTools.get(id);
	}

	// private String pythonPath;
	// private static String path;
	private static Map<String, Map<String, GalaxyTool>> galaxyTools;
	private static Map<String, GalaxyDataType> galaxyDataTypes;
	private static Map<String, GalaxyDataTable> galaxyDataTables;

	public GalaxyApplicationMaster() {
		super();
		// path = "$PATH";
	}

	@Override
	public void parseWorkflow() {
	}

	public static void parseWorkflow(String _fileName) {
		Map<String, GalaxyData> files = new HashMap<>();

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
						GalaxyData data = new GalaxyData(name);

						if (name.contains(".")) {
							String extension = name.substring(name.indexOf(".") + 1);
							if (galaxyDataTypes.containsKey(extension)) {
								data.setDataType(galaxyDataTypes.get(extension));
							}
						}

						String idName = id + "_output";
						data.setInput(true);
						files.put(idName, data);
					}
				} else if (type.equals("tool")) {
					String toolVersion = step.getString("tool_version");
					String toolId = step.getString("tool_id");
					String[] splitId = toolId.split("/");
					if (splitId.length > 2)
						toolId = splitId[splitId.length - 2];
					GalaxyTool tool = galaxyTools.get(toolId).get(toolVersion);
					if (tool == null) {
						System.err.println("Tool " + toolId + "/" + toolVersion + " could not be located in local Galaxy installation.");
						// onError(new RuntimeException());
						System.exit(-1);
					}

					GalaxyTaskInstance task = new GalaxyTaskInstance(id, tool.getName(), tool);
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
								if (newname.contains(" "))
									newname = "\"" + newname + "\"";
								renameOutputs.put(output_name, newname);
							} else if (action_type.equals("HideDatasetAction")) {
								String output_name = post_job_action.getString("output_name");
								hideOutputs.add(output_name);
							}
						}
					}

					task.addToolState(step.getString("tool_state"));

					Map<String, String> inputNameToIdName = new HashMap<>();
					JSONObject input_connections = step.getJSONObject("input_connections");
					for (String input_name : JSONObject.getNames(input_connections)) {
						JSONObject input_connection = input_connections.getJSONObject(input_name);
						inputNameToIdName.put(input_name, input_connection.getString("id") + "_" + input_connection.getString("output_name"));
					}

					JSONArray outputs = step.getJSONArray("outputs");
					List<String> outputFiles = new LinkedList<>();
					for (int j = 0; j < outputs.length(); j++) {
						JSONObject output = outputs.getJSONObject(j);
						String outputName = output.getString("name");

						String fileName = id + "_" + outputName;
						GalaxyDataType dataType = null;

						GalaxyParamValue param = tool.getFirstMatchingParamByName(outputName);
						String outputTypeString = param.getDataType();
						if (galaxyDataTypes.containsKey(outputTypeString)) {
							dataType = galaxyDataTypes.get(outputTypeString);
						} else if (inputNameToIdName.containsKey(outputTypeString)) {
							dataType = files.get(inputNameToIdName.get(outputTypeString)).getDataType();
						} else if (outputTypeString.equals("input")) {
							dataType = files.get(inputNameToIdName.values().iterator().next()).getDataType();
						}

						if (dataType != null) {
							String extension = dataType.getExtension();
							if (extension != null && extension.length() > 0) {
								fileName = fileName + "." + extension;
							}
						}
						if (renameOutputs.containsKey(outputName))
							fileName = renameOutputs.get(outputName);

						if (param.hasFrom_work_dir())
							task.addToPostScript("mv " + param.getFrom_work_dir() + " " + fileName);

						GalaxyData data = new GalaxyData(fileName);
						data.setDataType(dataType);

						String idName = id + "_" + outputName;
						if (!hideOutputs.contains(outputName)) {
							data.setOutput(true);
						}
						files.put(idName, data);
						task.addOutputData(data);
						task.addFile(outputName, data);
						outputFiles.add(fileName);
					}

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
						task.addFile(input_connection_key, files.get(idName));
						continue;
					}

					// (a) Build pickle python script
					task.buildPickleScript();
					task.buildTemplate();

					/* JSONObject tool_state = new JSONObject(tool_state_s); Map<String, String> params = new HashMap<>(); for (Iterator<?> it =
					 * tool_state.keys(); it.hasNext();) { String key = (String) it.next(); String value = tool_state.getString(key).replaceAll("\\\"", "");
					 * params.put(key, value); }
					 * 
					 * 
					 * 
					 * String command = tool.getTemplate(); System.out.println(command); Pattern paramPattern = Pattern.compile("\\$[^ ,]*"); Matcher
					 * paramMatcher = paramPattern.matcher(command); while (paramMatcher.find()) { String match = paramMatcher.group(); String matchRegEx =
					 * match.replace("$", "\\$").replace("{", "\\{").replace("}", "\\}"); System.out.print("\t" + match); String paramName = match.replace("$",
					 * "").replace("{", "").replace("}", "");
					 * 
					 * if (tool.hasInputTypes(paramName)) { command = command.replaceAll(matchRegEx, task.getDataByName(paramName).getLocalPath());
					 * System.out.println(" <-- " + task.getDataByName(paramName).getLocalPath()); continue; }
					 * 
					 * if (tool.hasOutputTypes(paramName)) { command = command.replaceAll(matchRegEx, task.getDataByName(paramName).getLocalPath());
					 * System.out.println(" <-- " + task.getDataByName(paramName).getLocalPath()); continue; }
					 * 
					 * if (paramName.contains(".metadata.")) { String[] splitParamName = paramName.split("\\."); Set<GalaxyDataType> dataTypes =
					 * tool.hasInputTypes(splitParamName[0]) ? tool.getInputTypes(splitParamName[0]) : tool.getOutputTypes(splitParamName[0]); for
					 * (GalaxyDataType dataType : dataTypes) { if (dataType.hasMetadata(splitParamName[2])) { command = command.replaceAll(matchRegEx,
					 * dataType.getMetadata(splitParamName[2])); System.out.println(" <-- " + dataType.getMetadata(splitParamName[2])); break; } } continue; }
					 * 
					 * if (params.containsKey(paramName)) { command = command.replace(match, params.get(paramName)); System.out.println(" <-- " +
					 * params.get(paramName)); continue; }
					 * 
					 * if (tool.hasDefaultParameter(paramName)) { command = command.replace(match, tool.getDefaultParameter(paramName));
					 * System.out.println(" <-- " + tool.getDefaultParameter(paramName)); continue; } }
					 * 
					 * System.out.println(command); task.setCommand(command); task.getReport().add( new JsonReportEntry(task.getWorkflowId(), task.getTaskId(),
					 * task.getTaskName(), task.getLanguageLabel(), task.getId(), null, JsonReportEntry.KEY_INVOC_SCRIPT, task.getCommand())); */
				}
			}

		} catch (IOException | JSONException | WorkflowStructureUnknownException e) {
			e.printStackTrace();
			// onError(e);
		}

		// scheduler.addTasks(tasks.values());
	}
}
