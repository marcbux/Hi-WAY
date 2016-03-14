package de.huberlin.wbi.hiway.am.cuneiforme;

import java.io.IOException;

import org.json.JSONException;

import de.huberlin.wbi.cfjava.cuneiform.Reply;
import de.huberlin.wbi.hiway.common.Worker;

public class CuneiformEWorker extends Worker {
	
	@Override
	public void stageOut() throws IOException, JSONException {
		Reply reply = Reply.createReply("reply_" + id);
	}

}
