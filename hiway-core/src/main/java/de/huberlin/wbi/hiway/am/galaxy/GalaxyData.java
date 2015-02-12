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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import de.huberlin.wbi.hiway.common.Data;

/**
 * An object that extends the Hi-WAY data object with a Galaxy data type.
 * 
 * @author Marc Bux
 *
 */
public class GalaxyData extends Data {
	private GalaxyDataType dataType;

	public GalaxyData(Path localPath, FileSystem fs) {
		super(localPath, fs);
	}

	public GalaxyData(String localPathString, FileSystem fs) {
		super(localPathString, fs);
	}

	public GalaxyDataType getDataType() {
		return dataType;
	}

	public boolean hasDataType() {
		return dataType != null;
	}

	public void setDataType(GalaxyDataType dataType) {
		this.dataType = dataType;
	}
}
