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

/**
 * Files processed by a Galaxy workflow always have their own data type. This data type determines the files' metadata, e.g. how many columns a CSV file has.
 * 
 * @author Marc Bux
 *
 */
public class GalaxyDataType {
	// the extension for files of this data type (e.g., CSV)
	private final String extension;
	// the path to the python file associated with this data type; this python file can be used to determine a to-be-processed file's metadata
	private final String file;
	// this data types (python class) name (e.g., Tabular)
	private final String name;

	public GalaxyDataType(String file, String name, String extension) {
		this.file = file;
		this.name = name;
		this.extension = extension;
	}

	public String getExtension() {
		return extension;
	}

	public String getFile() {
		return file;
	}

	public String getName() {
		return name;
	}

}
