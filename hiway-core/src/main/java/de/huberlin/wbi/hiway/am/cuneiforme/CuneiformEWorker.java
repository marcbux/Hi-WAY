package de.huberlin.wbi.hiway.am.cuneiforme;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.json.JSONException;

import de.huberlin.wbi.cfjava.cuneiform.Reply;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.Worker;

public class CuneiformEWorker extends Worker {
	
	@Override
	public void stageOut() throws IOException, JSONException {
		StringBuilder sb = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader("reply_" + id))) {
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		Reply reply = Reply.createReply(sb.toString());
		for (String fileNameString : reply.getStageOutFilenameList()) {
			outputFiles.add(new Data(fileNameString, containerId));
		}
		(new Data("reply_" + id)).stageOut();
		super.stageOut();
	}

}
