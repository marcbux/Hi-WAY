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
package de.huberlin.wbi.hiway.common;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * A file stored locally and in HDFS. HDFS directory paths are all relative to the HDFS user directory.
 * 
 * @author Marc Bux
 */
public class Data implements Comparable<Data> {
	private static FileSystem hdfs;
	private static Path hdfsApplicationDirectory;

	private static Path hdfsBaseDirectory;

	private static FileSystem localFs = new LocalFileSystem();

	public static void setHdfs(FileSystem hdfs) {
		Data.hdfs = hdfs;
	}

	public static void setHdfsApplicationDirectory(Path hdfsApplicationDirectory) {
		Data.hdfsApplicationDirectory = hdfsApplicationDirectory;
	}

	public static void setHdfsBaseDirectory(Path hdfsBaseDirectory) {
		Data.hdfsBaseDirectory = hdfsBaseDirectory;
	}

	private String containerId;

	private String fileName;

	private Path localDirectory;

	// is the file output of the workflow
	private boolean output;

	public Data(Path localPath) {
		this(localPath, null);
	}

	public Data(Path localPath, String containerId) {
		this.output = false;

		this.localDirectory = localPath.getParent();
		this.fileName = localPath.getName();
		this.containerId = containerId;
	}

	public Data(String localPathString) {
		this(new Path(localPathString), null);
	}

	public Data(String localPathString, String containerId) {
		this(new Path(localPathString), containerId);
	}

	public void addToLocalResourceMap(Map<String, LocalResource> localResources) throws IOException {
		Path dest = getHdfsPath();

		LocalResource rsrc = Records.newRecord(LocalResource.class);
		rsrc.setType(LocalResourceType.FILE);
		rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		rsrc.setResource(ConverterUtils.getYarnUrlFromPath(dest));

		FileStatus status = hdfs.getFileStatus(dest);
		rsrc.setTimestamp(status.getModificationTime());
		rsrc.setSize(status.getLen());

		localResources.put(getLocalPath().toString(), rsrc);
	}

	@Override
	public int compareTo(Data other) {
		return this.getLocalPath().compareTo(other.getLocalPath());
	}

	public long countAvailableLocalData(Container container) throws IOException {
		BlockLocation[] blockLocations = null;

		Path hdfsLocation = getHdfsPath();
		while (blockLocations == null) {
			FileStatus fileStatus = hdfs.getFileStatus(hdfsLocation);
			blockLocations = hdfs.getFileBlockLocations(hdfsLocation, 0, fileStatus.getLen());
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

	public long countAvailableTotalData() throws IOException {
		Path hdfsLocation = getHdfsPath();
		FileStatus fileStatus = hdfs.getFileStatus(hdfsLocation);
		return fileStatus.getLen();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof Data ? this.getLocalPath().equals(((Data) obj).getLocalPath()) : false;
	}

	public String getContainerId() {
		return containerId;
	}

	public Path getHdfsPath() {
		// if a container id has been created, this file is intermediate and will be found in the container's folder
		if (containerId != null) {
			return new Path (new Path(hdfsApplicationDirectory, containerId), fileName);
		}
		
		// else, we should check if the file is an input file; if so, it can be found directly in the hdfs base directory 
		Path basePath = new Path (hdfsBaseDirectory, fileName);
		try {
			if (hdfs.exists(basePath)) {
				return basePath;
			}
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		
		// otherwise, it is fair to assume that the file will be found in the application's folder
		return new Path (hdfsApplicationDirectory, fileName);
	}

	public Path getLocalPath() {
		return new Path(localDirectory, fileName);
	}

	public String getName() {
		return fileName;
	}

	@Override
	public int hashCode() {
		return this.getLocalPath().hashCode();
	}

	public boolean isOutput() {
		return output;
	}

	private void mkHdfsDir(Path dir) throws IOException {
		if (dir == null || hdfs.isDirectory(dir))
			return;
		mkHdfsDir(dir.getParent());
		hdfs.mkdirs(dir);
		hdfs.setPermission(dir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
	}

	public void setContainerId(String containerId) {
		this.containerId = containerId;
	}

	public void setOutput(boolean output) {
		this.output = output;
	}

	public void stageIn() throws IOException {
		Path hdfsPath = getHdfsPath();
		Path localPath = getLocalPath();
		if (localDirectory.depth() > 0) {
			localFs.mkdirs(localDirectory);
		}
		hdfs.copyToLocalFile(false, hdfsPath, localPath);
	}

	public void stageOut() throws IOException {
		Path localPath = getLocalPath();
		Path hdfsDirectory = getHdfsPath().getParent();
		Path hdfsPath = getHdfsPath();
		if (hdfsDirectory.depth() > 0) {
			mkHdfsDir(hdfsDirectory);
		}
		hdfs.copyFromLocalFile(false, true, localPath, hdfsPath);
	}

	@Override
	public String toString() {
		return getLocalPath().toString();
	}

}
