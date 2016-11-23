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
package de.huberlin.wbi.hiway.am.dax;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.common.WorkflowStructureUnknownException;

public class DaxApplicationMaster extends WorkflowDriver {

	public static void main(String[] args) {
		WorkflowDriver.launch(new DaxApplicationMaster(), args);
	}

	public DaxApplicationMaster() {
		super();
		setDetermineFileSizes();
	}

	@Override
	public Collection<TaskInstance> parseWorkflow() {
		Map<Object, TaskInstance> tasks = new HashMap<>();
		WorkflowDriver.writeToStdout("Parsing Pegasus DAX " + getWorkflowFile());

		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = builder.parse(new File(getWorkflowFile().getLocalPath().toString()));
			NodeList jobNds = doc.getElementsByTagName("job");

			for (int i = 0; i < jobNds.getLength(); i++) {
				Element jobEl = (Element) jobNds.item(i);
				String id = jobEl.getAttribute("id");
				String taskName = jobEl.getAttribute("name");
				DaxTaskInstance task = new DaxTaskInstance(getRunId(), taskName);
				task.setRuntime(jobEl.hasAttribute("runtime") ? Double.parseDouble(jobEl.getAttribute("runtime")) : 0d);
				tasks.put(id, task);

				StringBuilder arguments = new StringBuilder();
				NodeList argumentNds = jobEl.getElementsByTagName("argument");
				for (int j = 0; j < argumentNds.getLength(); j++) {
					Element argumentEl = (Element) argumentNds.item(j);

					NodeList argumentChildNds = argumentEl.getChildNodes();
					for (int k = 0; k < argumentChildNds.getLength(); k++) {
						Node argumentChildNd = argumentChildNds.item(k);
						String argument = "";

						switch (argumentChildNd.getNodeType()) {
						case Node.ELEMENT_NODE:
							Element argumentChildEl = (Element) argumentChildNd;
							if (argumentChildEl.getNodeName().equals("file")) {
								if (argumentChildEl.hasAttribute("name")) {
									argument = argumentChildEl.getAttribute("name");
								}
							} else if (argumentChildEl.getNodeName().equals("filename")) {
								if (argumentChildEl.hasAttribute("file")) {
									argument = argumentChildEl.getAttribute("file");
								}
							}
							break;
						case Node.TEXT_NODE:
							argument = argumentChildNd.getNodeValue().replaceAll("\\s+", " ").trim();
							break;
						default:
						}

						if (argument.length() > 0) {
							arguments.append(" ").append(argument);
						}
					}
				}

				NodeList usesNds = jobEl.getElementsByTagName("uses");
				for (int j = 0; j < usesNds.getLength(); j++) {
					Element usesEl = (Element) usesNds.item(j);
					if (usesEl.hasAttribute("type") && usesEl.getAttribute("type").compareTo("executable") == 0)
						continue;
					String link = usesEl.getAttribute("link");
					String fileName = usesEl.getAttribute("file");
					long size = usesEl.hasAttribute("size") ? Long.parseLong(usesEl.getAttribute("size")) : 0l;
					List<String> outputs = new LinkedList<>();

					switch (link) {
					case "input":
						if (!getFiles().containsKey(fileName)) {
							Data data = new Data(fileName);
							getFiles().put(fileName, data);
						}
						Data data = getFiles().get(fileName);
						task.addInputData(data, size);
						break;
					case "output":
						if (!getFiles().containsKey(fileName))
							getFiles().put(fileName, new Data(fileName));
						data = getFiles().get(fileName);
						if (!task.getInputData().contains(data)) {
							task.addOutputData(data, size);
						}
						outputs.add(fileName);
						break;
					default:
					}

					task.getReport().add(
							new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(),
									Long.valueOf(task.getId()), null, JsonReportEntry.KEY_INVOC_OUTPUT, new JSONObject().put("output", outputs)));
				}

				task.setCommand(taskName + arguments.toString());
				if (HiWayConfiguration.verbose)
					WorkflowDriver.writeToStdout("Adding task " + task + ": " + task.getInputData() + " -> " + task.getOutputData());
			}

			NodeList childNds = doc.getElementsByTagName("child");
			for (int i = 0; i < childNds.getLength(); i++) {
				Element childEl = (Element) childNds.item(i);
				String childId = childEl.getAttribute("ref");
				TaskInstance child = tasks.get(childId);

				NodeList parentNds = childEl.getElementsByTagName("parent");
				for (int j = 0; j < parentNds.getLength(); j++) {
					Element parentEl = (Element) parentNds.item(j);
					String parentId = parentEl.getAttribute("ref");
					TaskInstance parent = tasks.get(parentId);

					child.addParentTask(parent);
					parent.addChildTask(child);
				}
			}

			for (TaskInstance task : tasks.values()) {
				if (task.getChildTasks().size() == 0) {
					for (Data data : task.getOutputData()) {
						data.setOutput(true);
					}
				}

				task.getReport().add(
						new JsonReportEntry(task.getWorkflowId(), task.getTaskId(), task.getTaskName(), task.getLanguageLabel(), Long.valueOf(task.getId()),
								null, JsonReportEntry.KEY_INVOC_SCRIPT, task.getCommand()));
			}

		} catch (WorkflowStructureUnknownException | IOException | JSONException | ParserConfigurationException | SAXException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		return tasks.values();
	}
}
