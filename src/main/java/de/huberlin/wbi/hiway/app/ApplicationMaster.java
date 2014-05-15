package de.huberlin.wbi.hiway.app;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;

import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.TaskInstance;
import de.huberlin.wbi.hiway.scheduler.Scheduler;

public interface ApplicationMaster {
	
	public void buildScripts(TaskInstance task, Container container) throws IOException;
	
	public Collection<Data> getOutputFiles();
	
	public String getRunId();
	
	public Scheduler getScheduler();
	
	public String getWorkflowName();
	
	public boolean init(String[] args) throws ParseException;
	
	public void parseWorkflow();
	
	public boolean run() throws YarnException, IOException;
	
	public void taskFailure(TaskInstance task, ContainerId containerId);
	
	public void taskSuccess(TaskInstance task, ContainerId containerId);

}
