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
package de.huberlin.wbi.hiway.am.cuneiform;

import java.util.ArrayList;
import java.util.Collection;

import de.huberlin.wbi.cuneiform.core.cre.BaseCreActor;
import de.huberlin.wbi.cuneiform.core.cre.TicketReadyMsg;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.semanticmodel.NotBoundException;
import de.huberlin.wbi.cuneiform.core.semanticmodel.NotDerivableException;
import de.huberlin.wbi.cuneiform.core.semanticmodel.Ticket;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;

//Cre - Cuneiform Runtime Environment
public class HiWayCreActor extends BaseCreActor {

	private CuneiformApplicationMaster am;

	public HiWayCreActor(CuneiformApplicationMaster am) {
		super();
		this.am = am;
	}

	@Override
	public void processMsg(TicketReadyMsg msg) {

		Ticket ticket = msg.getTicket();

		Invocation invoc = Invocation.createInvocation(ticket);
		TaskInstance task = new CuneiformTaskInstance(invoc);

		try {
			for (String inputName : invoc.getStageInList()) {

				if (!am.getFiles().containsKey(inputName)) {
					Data data = new Data(inputName);
					data.setInput(true);
					am.getFiles().put(inputName, data);
				}
				Data data = am.getFiles().get(inputName);
				task.addInputData(data);
			}
		} catch (NotDerivableException e) {
			CuneiformApplicationMaster.onError(e);
		}

		try {
			task.setCommand(invoc.toScript());
			am.writeEntryToLog(invoc.getExecutableLogEntry());
			am.writeEntryToLog(invoc.getScriptLogEntry());
		} catch (NotBoundException | NotDerivableException e) {
			CuneiformApplicationMaster.onError(e);
		}

		Collection<TaskInstance> tasks = new ArrayList<>();
		tasks.add(task);
		am.getScheduler().addTasks(tasks);
	}

	@Override
	protected void shutdown() {
	}

}
