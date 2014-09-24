/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * J�rgen Brandt (HU Berlin)
 * Marc Bux (HU Berlin)
 * Ulf Leser (HU Berlin)
 *
 * J�rgen Brandt is funded by the European Commission through the BiobankCloud
 * project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 * research training group SOAMED (GRK 1651).
 *
 * Copyright 2014 Humboldt-Universit�t zu Berlin
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
package de.huberlin.wbi.hiway.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * A file stored locally and in HDFS. HDFS directory paths are all relative to
 * the HDFS user directory.
 * 
 * @author Marc Bux
 */
public class Data implements Comparable<Data> {
	// The hdfs directory midfixes (usually application and container id) for
	// non-input files, e.g. for a
	// non-input-file:
	// local: my_folder/my_file.txt
	// hdfs: sandbox/app_01/container_01/my_folder/my_file.txt
	public static Map<Data, String> hdfsDirectoryMidfixes = new HashMap<>();

	// The hdfs directory prefix (usually the Hi-WAY sandbox folder) for
	// non-input files
	private static String hdfsDirectoryPrefix = "";
	private static final Log log = LogFactory.getLog(Data.class);
	
	private String absoluteLocalDirectoryPrefix = "";

	public static String getHdfsDirectoryPrefix() {
		return hdfsDirectoryPrefix;
	}

	public static void setHdfsDirectoryPrefix(String hdfsDirectoryPrefixDefault) {
		Data.hdfsDirectoryPrefix = hdfsDirectoryPrefixDefault;
	}

	// The local directory and name of the file. The local directory is also the
	// suffix of the directoy in HDFS.
	private String directorySuffix = "";
	// is the file input or output of the workflow (otherwise, its an
	// intermediate and possibly temporary file product)
	private boolean input;

	private String name;

	private boolean output;

	public Data(String path) {
		input = false;
		output = false;

		int lastSlash = path.lastIndexOf("/");
		int endDirectory = (lastSlash > -1) ? lastSlash : 0;
		int startName = (lastSlash > -1) ? lastSlash + 1 : 0;
		String prefix = path.substring(0, endDirectory);
		
		if (path.startsWith("/")) {
			this.absoluteLocalDirectoryPrefix = prefix;
		} else {
			this.directorySuffix = prefix;
		}
		this.name = path.substring(startName);
	}

	public void addToLocalResourceMap(
			Map<String, LocalResource> localResources, FileSystem fs,
			String hdfsDirectoryMidfix) throws IOException {
		Path dest = new Path(fs.getHomeDirectory(),
				getHdfsPath(hdfsDirectoryMidfix));

		LocalResource rsrc = Records.newRecord(LocalResource.class);
		rsrc.setType(LocalResourceType.FILE);
		rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		rsrc.setResource(ConverterUtils.getYarnUrlFromPath(dest));

		FileStatus status = fs.getFileStatus(dest);
		rsrc.setTimestamp(status.getModificationTime());
		rsrc.setSize(status.getLen());

		localResources.put(getLocalPath(), rsrc);
	}

	@Override
	public int compareTo(Data other) {
		return this.getLocalPath().compareTo(other.getLocalPath());
	}

	public long countAvailableLocalData(FileSystem fs, Container container)
			throws IOException {
		BlockLocation[] blockLocations = null;

		String hdfsDirectoryMidfix = Data.hdfsDirectoryMidfixes
				.containsKey(this) ? Data.hdfsDirectoryMidfixes.get(this) : "";
		Path hdfsLocation = new Path(fs.getHomeDirectory(),
				getHdfsPath(hdfsDirectoryMidfix));
		while (blockLocations == null) {
			FileStatus fileStatus = fs.getFileStatus(hdfsLocation);
			blockLocations = fs.getFileBlockLocations(hdfsLocation, 0,
					fileStatus.getLen());
		}

		long sum = 0;
		for (BlockLocation blockLocation : blockLocations) {
			for (String host : blockLocation.getHosts()) {
				if (container.getNodeId().getHost().equals(host)) {
					sum += blockLocation.getLength();
					break;
				}
			}
		}
		return sum;
	}

	public long countAvailableTotalData(FileSystem fs) throws IOException {
		String hdfsDirectoryMidfix = Data.hdfsDirectoryMidfixes
				.containsKey(this) ? Data.hdfsDirectoryMidfixes.get(this) : "";
		Path hdfsLocation = new Path(fs.getHomeDirectory(),
				getHdfsPath(hdfsDirectoryMidfix));
		FileStatus fileStatus = fs.getFileStatus(hdfsLocation);
		return fileStatus.getLen();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof Data ? this.getLocalPath().equals(
				((Data) obj).getLocalPath()) : false;
	}

	public String getHdfsDirectory(String hdfsDirectoryMidfix) {
		List<String> directories = new ArrayList<>();
		if (!isInput() && hdfsDirectoryPrefix.length() > 0)
			directories.add(hdfsDirectoryPrefix);
		if (!isInput() && hdfsDirectoryMidfix.length() > 0)
			directories.add(hdfsDirectoryMidfix);
		if (directorySuffix.length() > 0)
			directories.add(directorySuffix);
		String hdfsDirectory = directories.size() > 0 ? directories.get(0) : "";
		for (int i = 1; i < directories.size(); i++) {
			hdfsDirectory = hdfsDirectory + "/" + directories.get(i);
		}
		return hdfsDirectory;
	}

	public String getHdfsPath(String hdfsDirectoryMidfix) {
		String hdfsDirectory = getHdfsDirectory(hdfsDirectoryMidfix);
		return (hdfsDirectory.length() == 0) ? name : hdfsDirectory + "/"
				+ name;
	}

	public String getLocalDirectory() {
		return directorySuffix;
	}

	public String getLocalPath() {
		String localDirectory = getLocalDirectory();
		return (localDirectory.length() == 0) ? name : localDirectory + "/"
				+ name;
	}

	public String getName() {
		return name;
	}

	@Override
	public int hashCode() {
		return this.getLocalPath().hashCode();
	}

	public boolean isInput() {
		return input;
	}

	public boolean isOutput() {
		return output;
	}

	public void setInput(boolean isInput) {
		this.input = isInput;
	}

	public void setOutput(boolean isOutput) {
		this.output = isOutput;
	}

	public void stageIn(FileSystem fs, String hdfsDirectoryMidfix)
			throws IOException {
		Path src = new Path(fs.getHomeDirectory(),
				getHdfsPath(hdfsDirectoryMidfix));
		Path dest = new Path(absoluteLocalDirectoryPrefix.length() > 0 ? absoluteLocalDirectoryPrefix + "/" + name : getLocalPath());
		log.debug("Staging in: " + src + " -> " + dest);
		if (!getLocalDirectory().isEmpty()) {
			Path dir = new Path(getLocalDirectory());
			fs.mkdirs(dir);
		}

		fs.copyToLocalFile(false, src, dest);
	}

	public void stageOut(FileSystem fs, String hdfsDirectoryMidfix)
			throws IOException {
		Path src = new Path(absoluteLocalDirectoryPrefix.length() > 0 ? absoluteLocalDirectoryPrefix + "/" + name : getLocalPath());
		Path dest = new Path(fs.getHomeDirectory(),
				getHdfsPath(hdfsDirectoryMidfix));
		log.debug("Staging out: " + src + " -> " + dest);
		if (!getHdfsDirectory(hdfsDirectoryMidfix).isEmpty()) {
			Path dir = new Path(fs.getHomeDirectory(),
					getHdfsDirectory(hdfsDirectoryMidfix));
			fs.mkdirs(dir);
		}
		// short replication = 1;
		// fs.setReplication(dest, replication);
		fs.copyFromLocalFile(false, true, src, dest);
	}

	@Override
	public String toString() {
		return getLocalPath();
	}

}
