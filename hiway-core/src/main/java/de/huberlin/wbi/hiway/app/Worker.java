/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Jörgen Brandt (HU Berlin)
 * Marc Bux (HU Berlin)
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
package de.huberlin.wbi.hiway.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.hiwaydb.useDB.HiwayDBI;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;
import de.huberlin.wbi.hiway.common.Data;

public class Worker {

	public static void main(String[] args) {
		Worker worker = new Worker();
		try {
			worker.init(args);
			worker.run();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private String appId;
	private String containerId;
	private boolean determineFileSizes = false;
	private Path dir;
	FileSystem fs;
	protected HiWayConfiguration hiWayConf;
	private Set<Data> inputFiles;
	private String langLabel;

	private Set<Data> outputFiles;
	private long signature;

	private long taskId;
	private String taskName;

	private UUID workflowId;

	public Worker() {
		inputFiles = new HashSet<>();
		outputFiles = new HashSet<>();
	}

	private int exec() {
		File script = new File("./" + containerId + ".sh");
		script.setExecutable(true);
		ProcessBuilder processBuilder = new ProcessBuilder(script.getPath());
		processBuilder.directory(dir.toFile());
		Process process;
		int exitValue = -1;
		try {
			File stdOutFile = new File(Invocation.STDOUT_FILENAME);
			File stdErrFile = new File(Invocation.STDERR_FILENAME);
			processBuilder.redirectOutput(stdOutFile);
			processBuilder.redirectError(stdErrFile);
			process = processBuilder.start();
			exitValue = process.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return exitValue;
	}

	public void init(String[] args) throws ParseException {
		hiWayConf = new HiWayConfiguration();
		dir = Paths.get(".");

		Options opts = new Options();
		opts.addOption("appId", true, "Id of this Container's Application Master.");
		opts.addOption("containerId", true, "Id of this Container.");
		opts.addOption("workflowId", true, "");
		opts.addOption("taskId", true, "");
		opts.addOption("taskName", true, "");
		opts.addOption("langLabel", true, "");
		opts.addOption("signature", true, "");
		opts.addOption("input", true, "");
		opts.addOption("output", true, "");
		opts.addOption("size", false, "");

		CommandLine cliParser = new GnuParser().parse(opts, args);
		appId = cliParser.getOptionValue("appId");
		containerId = cliParser.getOptionValue("containerId");
		workflowId = UUID.fromString(cliParser.getOptionValue("workflowId"));
		taskId = Long.parseLong(cliParser.getOptionValue("taskId"));
		taskName = cliParser.getOptionValue("taskName");
		langLabel = cliParser.getOptionValue("langLabel");
		signature = Long.parseLong(cliParser.getOptionValue("signature"));
		if (cliParser.hasOption("input")) {
			for (String inputList : cliParser.getOptionValues("input")) {
				String[] inputElements = inputList.split(",");
				Data input = new Data(inputElements[0]);
				input.setInput(Boolean.parseBoolean(inputElements[1]));
				Data.hdfsDirectoryMidfixes.put(input, inputElements[2]);
				inputFiles.add(input);
			}
		}
		if (cliParser.hasOption("size")) {
			determineFileSizes = true;
		}
		if (cliParser.hasOption("output")) {
			for (String output : cliParser.getOptionValues("output")) {
				outputFiles.add(new Data(output));
			}
		}

		Data.setHdfsDirectoryPrefix(hiWayConf.get(HiWayConfiguration.HIWAY_AM_SANDBOX_DIRECTORY, HiWayConfiguration.HIWAY_AM_SANDBOX_DIRECTORY_DEFAULT) + "/"
				+ appId);

		Configuration conf = new YarnConfiguration();
		conf.addResource("core-site.xml");
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() throws IOException, JSONException {
		long tic = System.currentTimeMillis();
		stageIn();
		long toc = System.currentTimeMillis();
		JSONObject obj = new JSONObject();
		obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, signature, null, HiwayDBI.KEY_INVOC_TIME_STAGEIN, obj));

		tic = System.currentTimeMillis();
		int exitValue = exec();
		toc = System.currentTimeMillis();
		obj = new JSONObject();
		obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, signature, null, JsonReportEntry.KEY_INVOC_TIME, obj));

		tic = System.currentTimeMillis();
		new Data(Invocation.STDOUT_FILENAME).stageOut(fs, containerId);
		new Data(Invocation.STDERR_FILENAME).stageOut(fs, containerId);
		if (exitValue != 0) {
			System.exit(exitValue);
		}
		stageOut();
		toc = System.currentTimeMillis();
		obj = new JSONObject();
		obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
		writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, signature, null, HiwayDBI.KEY_INVOC_TIME_STAGEOUT, obj));

		new Data(Invocation.REPORT_FILENAME).stageOut(fs, containerId);
	}

	public void stageIn() throws IOException, JSONException {
		for (Data input : inputFiles) {
			long tic = System.currentTimeMillis();
			input.stageIn(fs, Data.hdfsDirectoryMidfixes.get(input));
			long toc = System.currentTimeMillis();
			JSONObject obj = new JSONObject();
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
			writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, signature, input.getLocalPath(), HiwayDBI.KEY_FILE_TIME_STAGEIN,
					obj));
			if (determineFileSizes) {
				writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, signature, input.getLocalPath(),
						JsonReportEntry.KEY_FILE_SIZE_STAGEIN, Long.toString((new File(input.getLocalPath())).length())));
			}

		}
	}

	public void stageOut() throws IOException, JSONException {
		try (BufferedReader logReader = new BufferedReader(new FileReader(new File(Invocation.REPORT_FILENAME)))) {
			String line;
			while ((line = logReader.readLine()) != null) {
				JsonReportEntry entry = new JsonReportEntry(line);
				if (entry.getKey().equals(JsonReportEntry.KEY_FILE_SIZE_STAGEOUT)) {
					outputFiles.add(new Data(entry.getFile()));
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		for (Data output : outputFiles) {
			long tic = System.currentTimeMillis();
			output.stageOut(fs, containerId);
			long toc = System.currentTimeMillis();
			JSONObject obj = new JSONObject();
			obj.put(JsonReportEntry.LABEL_REALTIME, Long.toString(toc - tic));
			writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, signature, output.getLocalPath(),
					HiwayDBI.KEY_FILE_TIME_STAGEOUT, obj));
			if (determineFileSizes) {
				writeEntryToLog(new JsonReportEntry(tic, workflowId, taskId, taskName, langLabel, signature, output.getLocalPath(),
						JsonReportEntry.KEY_FILE_SIZE_STAGEOUT, Long.toString((new File(output.getLocalPath())).length())));
			}
		}
	}

	protected void writeEntryToLog(JsonReportEntry entry) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(Invocation.REPORT_FILENAME), true))) {
			writer.write(entry.toString() + "\n");
		}
	}

}
