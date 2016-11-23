package de.huberlin.wbi.hiway.am.cuneiforme;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

import de.huberlin.wbi.cfjava.cuneiform.RemoteWorkflow;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.Worker;

public class CuneiformEWorker extends Worker {

	public static void main(String[] args) {
		Worker.loop(new CuneiformEWorker(), args);
	}

	@Override
	public void stageOut() {
		try {
			JSONObject request = CuneiformEApplicationMaster.parseEffiFile(id + "_request");
			JSONObject reply = CuneiformEApplicationMaster.parseEffiFile(id + "_reply");
			for (String fileNameString : RemoteWorkflow.getOutputSet(request, reply)) {
				outputFiles.add(new Data(fileNameString, containerId));
			}
			(new Data(id + "_reply", containerId)).stageOut();
		} catch (JSONException | IOException e) {
			e.printStackTrace(System.out);
		}
		super.stageOut();
	}

}
