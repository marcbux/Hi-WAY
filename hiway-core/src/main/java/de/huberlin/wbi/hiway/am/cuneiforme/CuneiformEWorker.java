package de.huberlin.wbi.hiway.am.cuneiforme;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import de.huberlin.wbi.cfjava.cuneiform.Reply;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.Worker;

public class CuneiformEWorker extends Worker {

	public static void main(String[] args) {
		Worker.loop(new CuneiformEWorker(), args);
	}

	@Override
	public void stageOut() {
		StringBuilder sb = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(id + "_reply"))) {
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		Reply reply = Reply.createReply(sb.toString());
		for (String fileNameString : reply.getStageOutFilenameList()) {
			outputFiles.add(new Data(fileNameString, containerId));
		}
		try {
			(new Data(id + "_reply", containerId)).stageOut();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		super.stageOut();
	}

}
