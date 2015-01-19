/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Hannes Schuh (HU Berlin)
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
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
package de.huberlin.hiwaydb.useDB;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.print.attribute.standard.DateTimeAtCompleted;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Projections;
import org.json.JSONObject;

import de.huberlin.hiwaydb.dal.Accesstime;
import de.huberlin.hiwaydb.dal.File;
import de.huberlin.hiwaydb.dal.Hiwayevent;
import de.huberlin.hiwaydb.dal.Inoutput;
import de.huberlin.hiwaydb.dal.Invocation;
import de.huberlin.hiwaydb.dal.Task;
import de.huberlin.hiwaydb.dal.Workflowrun;
import de.huberlin.wbi.cuneiform.core.semanticmodel.JsonReportEntry;

public class HiwayDB implements HiwayDBI {

	public static <T> List<T> castList(Class<? extends T> clazz, Collection<?> c) {
		List<T> r = new ArrayList<T>(c.size());
		for (Object o : c)
			r.add(clazz.cast(o));
		return r;
	}

	private String configFile = "hibernate.cfg.xml";

	private static final Log log = LogFactory.getLog(HiwayDB.class);

	private SessionFactory dbSessionFactory = null;
	private SessionFactory dbSessionFactoryMessung = null;

	private String dbURL;
	private String password;
	private String username;
	private long dbVolume;
	private String config;
	private String wfName;
	private String runIDat;

	public HiwayDB(String username, String password, String dbURL) {
		this.username = username;
		this.password = password;
		this.dbURL = dbURL;
		this.wfName = "";
		this.runIDat = "";

		this.config = "nix";

		dbSessionFactory = getSQLSession();
		dbSessionFactoryMessung = getSQLSessionMessung();

		Session session = dbSessionFactory.openSession();
		Transaction tx = null;

		try {
			tx = session.beginTransaction();

			dbVolume = (long) session.createCriteria(Workflowrun.class)
					.setProjection(Projections.rowCount()).uniqueResult();

			tx.commit();
		} catch (RuntimeException e) {
			if (tx != null)
				tx.rollback();
			throw e; // or display error message
		} finally {
			if (session.isOpen()) {
				session.close();
			}

		}

	}

	@Override
	public void logToDB(JsonReportEntry entry) {

		lineToDB(entry);
		//
		// if (i.isEmpty()) {
		// // log.info("Write successful!!!");
		// } else {
		// log.info("hiwayDB |Fehler: " + i);
		// }
	}

	@Override
	public Set<String> getHostNames() {
		Long tick = System.currentTimeMillis();
		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}

		Set<String> tempResult = new HashSet<String>();

		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		Session sess = dbSessionFactory.openSession();
		//Transaction tx = null;
		
		List<String> results = null;

		// Non-managed environment idiom with getCurrentSession()
		try {
			//tx = sess.beginTransaction();
		
			txMessung = sessAT.beginTransaction();
			Accesstime at = new Accesstime();

			at.setTick(tick);
			at.setFunktion("getHostNames");
			at.setInput("SQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);

			Query query = null;
			List<Invocation> resultsInvoc = null;

			String hql = "SELECT I.hostname FROM Invocation I where I.hostname!=null";
			query = sess.createQuery(hql);

			results = query.list();

			for (String i : results) {
				tempResult.add(i);
			}

			Long x = (long) tempResult.size();

			at.setReturnvolume(x);
			Long tock = System.currentTimeMillis();
			at.setTock(tock);
			at.setTicktockdif(tock - tick);
			at.setRunId(this.runIDat);
			at.setWfName(this.wfName);
			sessAT.save(at);
			//tx.commit();
			txMessung.commit();
		} catch (RuntimeException e) {
//			if (tx != null)
//				tx.rollback();
			throw e; // or display error message
		} finally {
			if (sess.isOpen()) {
				sess.close();
			}
			
			if (sessAT.isOpen()) {
				sessAT.close();
			}
		}

		return tempResult;

	}

	// @Override
	// public Collection<InvocStat> getLogEntriesForTask(long taskId) {
	// Long tick = System.currentTimeMillis();
	// if (dbSessionFactory == null) {
	// dbSessionFactory = getSQLSession();
	// }
	//
	// Session sess = dbSessionFactory.openSession();
	//
	// List<Invocation> resultsInvoc = new ArrayList<Invocation>();
	// Collection<InvocStat> resultList;
	// Transaction tx = null;
	//
	// try {
	// tx = sess.beginTransaction();
	//
	// Accesstime at = new Accesstime();
	//
	// at.setTick(tick);
	// at.setFunktion("getLogEntriesForTask");
	// at.setInput("SQL");
	// at.setDbvolume(dbVolume);
	// Query query = null;
	//
	// query = sess.createQuery("FROM Invocation I  WHERE I.task ="
	// + taskId);
	//
	// resultsInvoc = castList(Invocation.class, query.list());
	//
	// Long x = (long) resultsInvoc.size();
	//
	// at.setReturnvolume(x);
	// Long tock = System.currentTimeMillis();
	// at.setTock(tock);
	// at.setTicktockdif(tock - tick);
	// at.setRunId(this.runIDat);
	// at.setWfName(this.wfName);
	// sessAT.save(at);
	//
	// tx.commit();
	//
	// resultList = createInvocStat(resultsInvoc, null);
	// } catch (RuntimeException e) {
	// if (tx != null)
	// tx.rollback();
	// throw e; // or display error message
	// } finally {
	// if (sess.isOpen()) {
	// sess.close();
	// }
	// }
	//
	// return resultList;
	//
	// }

	@Override
	public Collection<InvocStat> getLogEntriesForTasks(Set<Long> taskIds) {
		Long tick = System.currentTimeMillis();

		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}
		List<Invocation> resultsInvoc = new ArrayList<Invocation>();
		Collection<InvocStat> resultList;
		Session sess = dbSessionFactory.openSession();
		Transaction tx = null;
		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		
		Accesstime at = new Accesstime();

		try {
		//	tx = sess.beginTransaction();
			txMessung = sessAT.beginTransaction();
			
			at.setTick(tick);
			at.setFunktion("getLogEntriesForTasks");
			at.setInput("SQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);

			Query query = null;

			String queryString = "FROM Invocation I  WHERE ";

			for (Long l : taskIds) {
				queryString += " I.task = " + l.toString() + " or ";
			}

			// System.out.println(queryString.substring(0, queryString.length()
			// - 4));

			query = sess.createQuery(queryString.substring(0,
					queryString.length() - 4));

			// join I.invocationId
			resultsInvoc = castList(Invocation.class, query.list());
			Long x = (long) resultsInvoc.size();

			at.setReturnvolume(x);
			//tx.commit();
			resultList = createInvocStat(resultsInvoc, null);

		} catch (RuntimeException e) {
//			if (tx != null)
//				tx.rollback();
			throw e; // or display error message
		} finally {
			if (sess.isOpen()) {
				sess.close();
			}
			
		}

		Long tock = System.currentTimeMillis();
		at.setTock(tock);
		at.setTicktockdif(tock - tick);
		at.setRunId(this.runIDat);
		at.setWfName(this.wfName);
		sessAT.save(at);
		
		txMessung.commit();
		
		if (sessAT.isOpen()) {
			sessAT.close();
		}
		
		return resultList;

	}

	@Override
	public Set<Long> getTaskIdsForWorkflow(String workflowName) {
		Long tick = System.currentTimeMillis();
		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}

		Set<Long> tempResult = new HashSet<Long>();

		Session sess = dbSessionFactory.openSession();
		//Transaction tx = null;
		Session sessAT = dbSessionFactoryMessung.openSession();
		
		Accesstime at = new Accesstime();

		Transaction txMessung = null;
		try {
		//	tx = sess.beginTransaction();
			txMessung = sessAT.beginTransaction();
		
			at.setTick(tick);
			at.setFunktion("getTaskIdsForWorkflow");
			at.setInput("SQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);

			Query query = null;

			query = sess.createQuery("FROM Workflowrun W WHERE W.wfname ='"
					+ workflowName + "'");

			List<Workflowrun> resultsWF = new ArrayList<Workflowrun>();

			// query = session
			// .createQuery("select new list(hostname)  FROM Invocation I");

			resultsWF = query.list();

			for (Workflowrun w : resultsWF) {
				// System.out.println("in getHostnames: " + i.getHostname());
				for (Invocation i : w.getInvocations()) {
					tempResult.add(i.getTask().getTaskId());
				}
			}

			Long x = (long) tempResult.size();

			at.setReturnvolume(x);	
	
			//tx.commit();
			
		} catch (RuntimeException e) {
//			if (tx != null)
//				tx.rollback();
			throw e; // or display error message
		} finally {
			if (sess.isOpen()) {
				sess.close();
			}
			
		}
		
		Long tock = System.currentTimeMillis();
		at.setTock(tock);
		at.setTicktockdif(tock - tick);
		at.setRunId(this.runIDat);
		at.setWfName(this.wfName);
		sessAT.save(at);
		
		txMessung.commit();
		
		if (sessAT.isOpen()) {
			sessAT.close();
		}

		return tempResult;
	}

	@Override
	public String getTaskName(long taskId) {
		Long tick = System.currentTimeMillis();
		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}

		List<Task> resultsInvoc = new ArrayList<Task>();

		Session sess = dbSessionFactory.openSession();
		//Transaction tx = null;
		String result = "";
		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		Accesstime at = new Accesstime();

		try {
			//tx = sess.beginTransaction();
			txMessung = sessAT.beginTransaction();
			
		

			at.setTick(tick);
			at.setFunktion("getTaskName");
			at.setWfName(wfName);
			at.setConfig(config);
			at.setInput("SQL");
			at.setDbvolume(dbVolume);

			Query query = null;

			query = sess.createQuery("FROM Task T  WHERE T.taskid =" + taskId);
			// join I.invocationId
			resultsInvoc = query.list();

			Long x = (long) resultsInvoc.size();

			at.setReturnvolume(x);
						
			if (!resultsInvoc.isEmpty()) {
				result = resultsInvoc.get(0).getTaskName();
			}

		} catch (RuntimeException e) {
//			if (tx != null)
//				tx.rollback();
			throw e; // or display error message
		} finally {
			if (sess.isOpen()) {
				sess.close();
			}
			
		}
		
		Long tock = System.currentTimeMillis();
		at.setTock(tock);
		at.setTicktockdif(tock - tick);
		at.setRunId(this.runIDat);
		at.setWfName(this.wfName);
		sessAT.save(at);

		txMessung.commit();
		
		if (sessAT.isOpen()) {
			sessAT.close();
		}
		
		return result;

	}

	private Collection<InvocStat> createInvocStat(List<Invocation> invocations,
			Session sess) {

		// log.info("CreateInvocStat from size:" + invocations.size());
		Set<InvocStat> resultList = new HashSet<InvocStat>();
		Invocation tempInvoc;

		for (int i = 0; i < invocations.size(); i++) {
			tempInvoc = invocations.get(i);

			// if (dbSessionFactory == null) {
			// dbSessionFactory = getSQLSession();
			// }
			//
			// session = dbSessionFactory.openSession();

			// Query query = null;
			// List<Workflowrun> resultsRun = null;
			//
			// query = session.createQuery("FROM Workflowrun T  WHERE T.id =" +
			// tempInvoc.getWorkflowrun().get());
			// // join I.invocationId
			// resultsRun = query.list();
			//
			//
			// String testuu = "00000000-9aa1-4a3c-9ad6-00000000000";
			//
			// if(!resultsRun.isEmpty())
			// {
			// testuu = resultsRun.get(0).getRunId();
			// }

			InvocStat invoc = new InvocStat(tempInvoc.getWorkflowrun()
					.getRunId(), tempInvoc.getTask().getTaskId());

			if (tempInvoc.getHostname() != null
					&& tempInvoc.getTask().getTaskId() != 0
					&& tempInvoc.getRealTime() != null) {
				invoc.setHostName(tempInvoc.getHostname());
				invoc.setRealTime(tempInvoc.getRealTime(),
						tempInvoc.getTimestamp());

				Set<FileStat> iFiles = new HashSet<FileStat>();
				Set<FileStat> oFiles = new HashSet<FileStat>();

				// Files
				for (File f : tempInvoc.getFiles()) {

					FileStat iFile = new FileStat();
					iFile.setFileName(f.getName());

					FileStat oFile = new FileStat();
					oFile.setFileName(f.getName());

					if (f.getRealTimeIn() != null) {
						iFile.setRealTime(f.getRealTimeIn());
						iFile.setSize(f.getSize());
						iFiles.add(iFile);
					}

					if (f.getRealTimeOut() != null) {
						oFile.setRealTime(f.getRealTimeOut());
						oFile.setSize(f.getSize());
						oFiles.add(oFile);
					}
				}

				invoc.setInputfiles(iFiles);
				invoc.setOutputfiles(oFiles);

				// log.info("hiwayDB |Invoc: " + tempInvoc.getInvocationId()
				// + " Host:" + tempInvoc.getHostname() + " Task:"
				// + tempInvoc.getTask().getTaskId() + " RealTime: "
				// + tempInvoc.getRealTime() + " Timestamp: "
				// + tempInvoc.getTimestamp());

				//
				// log.info("hiwayDB |Invoc mit Timestamp: "
				// + invoc.getTimestamp() + " geadded!");
				resultList.add(invoc);
			}
		}
		if (sess != null && sess.isOpen()) {
			sess.close();

			log.info("hiwayDB | Close Session  -> CreateInvocStat DONE ->Size: "
					+ resultList.size());
		}
			

		return resultList;
	}

	// @Override
	// public Collection<InvocStat> getLogEntriesForTaskOnHost(long taskId,
	// String hostName) {
	// Long tick = System.currentTimeMillis();
	// if (dbSessionFactory == null) {
	// dbSessionFactory = getSQLSession();
	// }
	//
	// List<Invocation> resultsInvoc = new ArrayList<Invocation>();
	// Collection<InvocStat> resultList;
	//
	// Session sess = dbSessionFactory.openSession();
	// Transaction tx = null;
	//
	// try {
	// tx = sess.beginTransaction();
	//
	// Accesstime at = new Accesstime();
	//
	// at.setTick(tick);
	// at.setFunktion("getLogEntriesForTaskOnHost");
	// at.setInput("SQL");
	// at.setDbvolume(dbVolume);
	//
	// Query query = null;
	//
	// query = sess.createQuery("FROM Invocation I  WHERE I.hostname ='"
	// + hostName + "' and I.task = " + taskId);
	//
	// resultsInvoc = castList(Invocation.class, query.list());
	// Long x = (long) resultsInvoc.size();
	//
	// at.setReturnvolume(x);
	// Long tock = System.currentTimeMillis();
	// at.setTock(tock);
	// at.setTicktockdif(tock - tick);
	// at.setRunId(this.runIDat);
	// at.setWfName(this.wfName);
	// sessAT.save(at);
	//
	// tx.commit();
	//
	// resultList = createInvocStat(resultsInvoc, null);
	//
	// } catch (RuntimeException e) {
	// if (tx != null)
	// tx.rollback();
	// throw e; // or display error message
	// } finally {
	// if (sess.isOpen()) {
	// sess.close();
	// }
	// }
	//
	// return resultList;
	// }

	@Override
	public Collection<InvocStat> getLogEntriesForTaskOnHostSince(long taskId,
			String hostName, long timestamp) {
		Long tick = System.currentTimeMillis();
		// log.info("beginn getLogEntriesForTaskOnHostSince... ");
		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}

		List<Invocation> resultsInvoc = new ArrayList<Invocation>();
		Collection<InvocStat> resultList;

		// log.info("getLogEntriesForTaskOnHostSince TID" + taskId + " Host: " +
		// hostName + " time: " + timestamp);
		// log.info("open Session");
		Session sess = dbSessionFactory.openSession();
		// log.info("weiter... und return");
		//Transaction tx = sess.getTransaction();
		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		Accesstime at = new Accesstime();


		try {
			// log.info("beginn TA");
			//tx = sess.beginTransaction();
			txMessung = sessAT.beginTransaction();
			// log.info("started TA for getLogEntriesForTaskOnHostSince ");
			
			at.setTick(tick);
			at.setFunktion("getLogEntriesForTaskOnHostSince");
			at.setInput("SQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);

			Query query = null;
			// log.info("set query...");

			query = sess.createQuery("FROM Invocation I  WHERE I.hostname ='"
					+ hostName + "' and I.Timestamp >" + timestamp
					+ " and I.task = " + taskId);

			// log.info("Suche mit Query: FROM Invocation I  WHERE I.hostname ='"
			// + hostName + "' and I.Timestamp >" + timestamp
			// + " and I.task = " + taskId);

			// query.setTimeout(2);
			// log.info("Q: " + query.toString());

			resultsInvoc = castList(Invocation.class, query.list());

			// log.info("Ergebnisse Size: " + resultsInvoc.size());

			Long x = (long) resultsInvoc.size();

			at.setReturnvolume(x);
			
			// log.info("commit");
			//tx.commit();
			
			resultList = createInvocStat(resultsInvoc, null);

		} catch (RuntimeException e) {
//			if (tx != null)
//				tx.rollback();
			throw e; // or display error message
		} finally {
			if (sess.isOpen()) {
				sess.close();
			}
			
		}

		Long tock = System.currentTimeMillis();
		at.setTock(tock);
		at.setTicktockdif(tock - tick);
		at.setRunId(this.runIDat);
		at.setWfName(this.wfName);
		sessAT.save(at);
	
		txMessung.commit();
		
		if (sessAT.isOpen()) {
			sessAT.close();
		}
		
		return resultList;
	}

	private void lineToDB(JsonReportEntry logEntryRow) {

		Long tick = System.currentTimeMillis();

		Session oneSession = dbSessionFactory.openSession();

		Transaction tx = oneSession.getTransaction();
		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		try {

			log.info("hiwayDB | start adding Entry time: " + tick);

			tx = oneSession.beginTransaction();
			txMessung = sessAT.beginTransaction();
			// log.info("hiwayDB | tx = " + tx.getLocalStatus() +
			// "    Session: " + oneSession.getStatistics());

			Accesstime at = new Accesstime();

			at.setTick(tick);
			at.setFunktion("JsonReportEntryToDB");
			at.setInput("SQL");
			at.setDbvolume(dbVolume);

			Query query = null;
			List<Task> resultsTasks = null;
			List<Workflowrun> resultsWfRun = null;
			List<File> resultsFile = new ArrayList();

			String runID = null;
			Long wfId = null;

			if (logEntryRow.getRunId() != null) {
				runID = logEntryRow.getRunId().toString();

				query = oneSession
						.createQuery("FROM Workflowrun E WHERE E.runid='"
								+ runID + "'");

				resultsWfRun = query.list();
			}

			Workflowrun wfRun = null;
			if (resultsWfRun != null && !resultsWfRun.isEmpty()) {

				wfRun = resultsWfRun.get(0);
				wfId = wfRun.getId();
				// log.info("WF run ist nicht empty:" + wfId);
			}

			long taskID = 0;
			if (logEntryRow.getTaskId() != null) {
				taskID = logEntryRow.getTaskId();

				query = oneSession.createQuery("FROM Task E WHERE E.taskid ="
						+ taskID);
				resultsTasks = query.list();
			}

			Long invocID = (long) 0;

			if (logEntryRow.hasInvocId()) {
				invocID = logEntryRow.getInvocId();
				log.info("Has InvovID: " + invocID);
			}

			query = oneSession
					.createQuery("FROM Invocation E WHERE E.invocationid ="
							+ invocID + " and E.workflowrun='" + wfId + "'");
			List<Invocation> resultsInvoc = castList(Invocation.class,
					query.list());

			Long timestampTemp = logEntryRow.getTimestamp();

			Task task = null;
			if (resultsTasks != null && !resultsTasks.isEmpty()) {
				task = resultsTasks.get(0);
			}
			Invocation invoc = null;
			if (resultsInvoc != null && !resultsInvoc.isEmpty()) {
				invoc = resultsInvoc.get(0);
				log.info("invoc gefunden: ID " + invoc.getInvocationId());
			}
			
			if (wfRun == null && runID != null) {

				wfRun = new Workflowrun();
				wfRun.setRunId(runID);
				oneSession.save(wfRun);
				log.info("hiwayDB | save WfRun: " + runID);
				this.runIDat = runID;
			}

			if (taskID != 0 && (task == null)) {
				task = new Task();

				task.setTaskId(taskID);
				task.setTaskName(logEntryRow.getTaskName());
				task.setLanguage(logEntryRow.getLang());

				oneSession.save(task);
				log.info("hiwayDB | save Task: " + taskID + " - "+ task.getTaskName());
				// System.out.println("Neuer.. Tasks in DB speichern ID: "
				// + task.getTaskId());
			}
			
			
			if (invocID != 0 && (invoc == null)) {
				invoc = new Invocation();
				invoc.setTimestamp(timestampTemp);
				invoc.setInvocationId(invocID);
				invoc.setTask(task);
				invoc.setWorkflowrun(wfRun);
				oneSession.save(invoc);
				log.info("hiwayDB | save Invoc: " + invocID);
			}

			String filename = null;
			log.info("getFile != null ?  :" + logEntryRow.getFile() != null);
			
			log.info("invoc != null ?  :" + invoc != null);
			
			if (logEntryRow.getFile() != null && invoc != null) {
				filename = logEntryRow.getFile();

				// query = session.createQuery("FROM File E WHERE E.name='"
				// + filename + "' AND E.invocation" + invocID);

				query = oneSession.createQuery("FROM File E WHERE E.name='"
						+ filename + "' AND E.invocation=" + invoc.getId());

				log.info("File Query:" + query.toString());
				// List<File> resultsFileTemp =
				resultsFile = query.list();

				// for (File f : resultsFileTemp) {
				// if (f.getInvocation().getInvocationId() == invocID) {
				// resultsFile.add(f);
				// }
				// }
			}

			File file = null;
			if (resultsFile != null && !resultsFile.isEmpty()) {
				file = resultsFile.get(0);
				log.info("File haben wir:" + file.getName() + file.getId());
			}

			

			
			if (file == null && filename != null) {

				file = new File();
				file.setName(filename);
				file.setInvocation(invoc);
				oneSession.save(file);
				log.info("hiwayDB | save File: " + filename);
			}

			String key = logEntryRow.getKey();

			// public static final String KEY_INVOC_TIME_STAGEIN =
			// "invoc-time-stagein";
			// public static final String KEY_INVOC_TIME_STAGEOUT =
			// "invoc-time-stageout";

			log.info("hiwayDB | save KEY: " + key);

			at.setKeyinput(key);

			JSONObject valuePart;
			switch (key) {
			case HiwayDBI.KEY_INVOC_HOST:
				invoc.setHostname(logEntryRow.getValueRawString());
				break;
			case "wf-name":
				wfRun.setWfName(logEntryRow.getValueRawString());
				this.wfName = logEntryRow.getValueRawString();
				break;
			case "wf-time":
				String val = logEntryRow.getValueRawString();
				Long test = Long.parseLong(val, 10);

				wfRun.setWfTime(test);
				break;
			case HiwayDBI.KEY_INVOC_TIME_SCHED:
				valuePart = logEntryRow.getValueJsonObj();
				invoc.setScheduleTime(GetTimeStat(valuePart));
				break;

			case JsonReportEntry.KEY_INVOC_STDERR:
				invoc.setStandardError(logEntryRow.getValueRawString());
				break;

			case JsonReportEntry.KEY_INVOC_SCRIPT:
				// valuePart = logEntryRow.getValueRawString();

				Inoutput input = new Inoutput();
				input.setKeypart("invoc-exec");
				input.setInvocation(invoc);
				input.setContent(logEntryRow.getValueRawString());
				input.setType("input");
				oneSession.save(input);
				break;

			case JsonReportEntry.KEY_INVOC_OUTPUT:
				valuePart = logEntryRow.getValueJsonObj();

				Inoutput output = new Inoutput();
				output.setKeypart("invoc-output");
				output.setInvocation(invoc);
				output.setContent(valuePart.toString());
				output.setType("output");
				oneSession.save(output);
				break;

			case JsonReportEntry.KEY_INVOC_STDOUT:
				invoc.setStandardOut(logEntryRow.getValueRawString());
				break;

			case "invoc-time-stagein":
				valuePart = logEntryRow.getValueJsonObj();

				invoc.setRealTimeIn(GetTimeStat(valuePart));

				// invoc.setRealTimeIn(realtimein)
				break;
			case "invoc-time-stageout":
				valuePart = logEntryRow.getValueJsonObj();

				invoc.setRealTimeOut(GetTimeStat(valuePart));

				// invoc.setRealTimeIn(realtimein)
				break;

			case HiwayDBI.KEY_FILE_TIME_STAGEIN:
				valuePart = logEntryRow.getValueJsonObj();
				
				file.setRealTimeIn(GetTimeStat(valuePart));

				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEOUT:
				valuePart = logEntryRow.getValueJsonObj();

				file.setRealTimeOut(GetTimeStat(valuePart));
				break;

			case JsonReportEntry.KEY_INVOC_TIME:
				valuePart = logEntryRow.getValueJsonObj();

				try {
					invoc.setRealTime(GetTimeStat(valuePart));
				} catch (NumberFormatException e) {
					invoc.setRealTime(1l);
				}

				break;
			case "file-size-stagein":

				file.setSize(Long.parseLong(logEntryRow.getValueRawString(), 10));

				break;
			case "file-size-stageout":
				file.setSize(Long.parseLong(logEntryRow.getValueRawString(), 10));

				break;
			case HiwayDBI.KEY_HIWAY_EVENT:
				valuePart = logEntryRow.getValueJsonObj();

				Hiwayevent he = new Hiwayevent();
				he.setWorkflowrun(wfRun);
				he.setContent(valuePart.toString());
				he.setType(valuePart.get("type").toString());
				oneSession.save(he);
				break;

			}
			
			tx.commit();

			at.setReturnvolume((long) logEntryRow.toString().length());
			Long tock = System.currentTimeMillis();
			at.setTock(tock);
			at.setTicktockdif(tock - tick);
			at.setWfName(this.wfName);
			at.setRunId(this.runIDat);

			sessAT.save(at);

			// log.info("hiwayDB | Commit " + tx.getLocalStatus() + " und " +
			// oneSession.getStatistics());
			
			txMessung.commit();
			// log.info("hiwayDB | nach comit " + tx.getLocalStatus() +
			// " und Session " + oneSession.isOpen() +" stat " +
			// oneSession.getStatistics());
			// session.getTransaction().commit();

		} catch (org.hibernate.exception.ConstraintViolationException e) {

			log.info("hiwayDB FEHLER | ConstraintViolationException: "
					+ e.getConstraintName());
			String message = e.getSQLException().getMessage();

			if (message.contains("RundID_UNIQUE")) {
				log.info("hiwayDB FEHLER | runIDUnique");
			} else if ((message.contains("JustOneFile"))) {
				log.info("hiwayDB FEHLER | JustOneFile");
			}
			logStackTrace(e);

			if (tx != null) {
				log.info("hiwayDB Rollback");
				tx.rollback();
				// session.getTransaction().rollback();
			}
			System.exit(1);

		} catch (Exception e) {
			if (tx != null) {
				log.info("hiwayDB Rollback");
				tx.rollback();
				// session.getTransaction().rollback();
			}
			log.info("hiwayDB FEHLER | " + e);

			logStackTrace(e);
			System.exit(1);

		} finally {
			if (oneSession.isOpen()) {
				// log.info("hiwayDB | Close Session  -> lineToDB DONE");
				oneSession.close();
			}
			if (sessAT.isOpen()) {
				sessAT.close();
			}
		}
	}

	private static Long GetTimeStat(JSONObject valuePart) {

		// Timestat timeStat = new Timestat();
		// timeStat.setNminPageFault(Long.parseLong(valuePart.get("nMinPageFault")
		// .toString(), 10));
		// timeStat.setNforcedContextSwitch(Long.parseLong(
		// valuePart.get("nForcedContextSwitch").toString(), 10));
		// timeStat.setAvgDataSize(Long.parseLong(valuePart.get("avgDataSize")
		// .toString(), 10));
		// timeStat.setNsocketRead(Long.parseLong(valuePart.get("nSocketRead")
		// .toString(), 10));
		// timeStat.setNioWrite(Long.parseLong(valuePart.get("nIoWrite")
		// .toString(), 10));
		// timeStat.setAvgResidentSetSize(Long.parseLong(
		// valuePart.get("avgResidentSetSize").toString(), 10));
		// timeStat.setNmajPageFault(Long.parseLong(valuePart.get("nMajPageFault")
		// .toString(), 10));
		// timeStat.setNwaitContextSwitch(Long.parseLong(
		// valuePart.get("nWaitContextSwitch").toString(), 10));
		// timeStat.setUserTime(Double.parseDouble(valuePart.get("userTime")
		// .toString()));
		// timeStat.setRealTime(Double.parseDouble(valuePart.get("realTime")
		// .toString()));
		// timeStat.setSysTime(Double.parseDouble(valuePart.get("sysTime")
		// .toString()));
		// timeStat.setNsocketWrite(Long.parseLong(valuePart.get("nSocketWrite")
		// .toString(), 10));
		// timeStat.setMaxResidentSetSize(Long.parseLong(
		// valuePart.get("maxResidentSetSize").toString(), 10));
		// timeStat.setAvgStackSize(Long.parseLong(valuePart.get("avgStackSize")
		// .toString(), 10));
		// timeStat.setNswapOutMainMem(Long.parseLong(
		// valuePart.get("nSwapOutMainMem").toString(), 10));
		// timeStat.setNioRead(Long.parseLong(valuePart.get("nIoRead").toString(),
		// 10));
		// timeStat.setNsignal(Long.parseLong(valuePart.get("nSignal").toString(),
		// 10));
		// timeStat.setAvgTextSize(Long.parseLong(valuePart.get("avgTextSize")
		// .toString(), 10));

		return Long.parseLong(valuePart.get("realTime").toString(), 10);
	}

	private SessionFactory getSQLSession() {
		try {

			if (dbURL != null && username != null) {

				Configuration configuration = new Configuration();
				// .configure(f);

				configuration.setProperty("hibernate.connection.url", dbURL);
				configuration.setProperty("hibernate.connection.username",
						username);
				if (this.password != null) {
					configuration.setProperty("hibernate.connection.password",
							this.password);
				} else {
					configuration.setProperty("hibernate.connection.password",
							"");
				}

				configuration.setProperty("hibernate.dialect",
						"org.hibernate.dialect.MySQLInnoDBDialect");
				configuration.setProperty("hibernate.connection.driver_class",
						"com.mysql.jdbc.Driver");

				// configuration.setProperty("hibernate.connection.pool_size","10");
				configuration.setProperty("connection.provider_class",
						"org.hibernate.connection.C3P0ConnectionProvider");

				// <property
				// name="hibernate.transaction.factory_class">org.hibernate.transaction.JDBCTransactionFactory</property>

				configuration.setProperty(
						"hibernate.transaction.factory_class",
						"org.hibernate.transaction.JDBCTransactionFactory");

				configuration.setProperty(
						"hibernate.current_session_context_class", "thread");

				configuration.setProperty("hibernate.initialPoolSize", "20");
				configuration.setProperty("hibernate.c3p0.min_size", "5");
				configuration.setProperty("hibernate.c3p0.max_size", "1000");

				configuration.setProperty("hibernate.maxIdleTime", "3600");
				configuration.setProperty(
						"hibernate.c3p0.maxIdleTimeExcessConnections", "300");

				// configuration.setProperty("hibernate.c3p0.testConnectionOnCheckout",
				// "false");
				configuration.setProperty("hibernate.c3p0.timeout", "330");
				configuration.setProperty("hibernate.c3p0.idle_test_period",
						"300");

				configuration.setProperty("hibernate.c3p0.max_statements",
						"13000");
				configuration.setProperty(
						"hibernate.c3p0.maxStatementsPerConnection", "30");

				configuration.setProperty("hibernate.c3p0.acquire_increment",
						"10");

				// <property name="hibernate.show_sql">true</property>
				// <property name="hibernate.use_sql_comments">true</property>

				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.Hiwayevent.class);
				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.File.class);
				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.Inoutput.class);
				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.Invocation.class);
				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.Task.class);
				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.Userevent.class);
				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.Workflowrun.class);
				configuration
						.addAnnotatedClass(de.huberlin.hiwaydb.dal.Accesstime.class);

				StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
						.applySettings(configuration.getProperties());
				SessionFactory sessionFactory = configuration
						.buildSessionFactory(builder.build());
				log.info("Session Factory Starten!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NEUE Tocks gesetzt");
				return sessionFactory;

			} else {
				java.io.File f = new java.io.File(configFile);

				Configuration configuration = new Configuration().configure(f);
				StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
						.applySettings(configuration.getProperties());
				SessionFactory sessionFactory = configuration
						.buildSessionFactory(builder.build());
				return sessionFactory;
			}

		} catch (Throwable ex) {
			System.err.println("Failed to create sessionFactory object." + ex);
			throw new ExceptionInInitializerError(ex);
		}

	}

	public void logStackTrace(Throwable e) {
		Writer writer = new StringWriter();
		PrintWriter printWriter = new PrintWriter(writer);
		e.printStackTrace(printWriter);
		log.error(writer.toString());
	}

	private SessionFactory getSQLSessionMessung() {
		try {

			String url = dbURL.substring(0, dbURL.lastIndexOf("/"))
					+ "/messungen";

			Configuration configuration = new Configuration();
			// .configure(f);

			configuration.setProperty("hibernate.connection.url", url);
			configuration
					.setProperty("hibernate.connection.username", username);
			if (this.password != null) {
				configuration.setProperty("hibernate.connection.password",
						this.password);
			} else {
				configuration.setProperty("hibernate.connection.password", "");
			}

			configuration.setProperty("hibernate.dialect",
					"org.hibernate.dialect.MySQLInnoDBDialect");
			configuration.setProperty("hibernate.connection.driver_class",
					"com.mysql.jdbc.Driver");
//			
//			configuration.setProperty("hibernate.show_sql",
//					"true");
			
			//<property name="hibernate.show_sql">true</property>

			// configuration.setProperty("hibernate.connection.pool_size","10");
			configuration.setProperty("connection.provider_class",
					"org.hibernate.connection.C3P0ConnectionProvider");

			// <property
			// name="hibernate.transaction.factory_class">org.hibernate.transaction.JDBCTransactionFactory</property>

			configuration.setProperty("hibernate.transaction.factory_class",
					"org.hibernate.transaction.JDBCTransactionFactory");

			configuration.setProperty(
					"hibernate.current_session_context_class", "thread");

			configuration.setProperty("hibernate.initialPoolSize", "20");
			configuration.setProperty("hibernate.c3p0.min_size", "5");
			configuration.setProperty("hibernate.c3p0.max_size", "1000");

			configuration.setProperty("hibernate.maxIdleTime", "3600");
			configuration.setProperty(
					"hibernate.c3p0.maxIdleTimeExcessConnections", "300");

			// configuration.setProperty("hibernate.c3p0.testConnectionOnCheckout",
			// "false");
			configuration.setProperty("hibernate.c3p0.timeout", "330");
			configuration.setProperty("hibernate.c3p0.idle_test_period", "300");

			configuration.setProperty("hibernate.c3p0.max_statements", "13000");
			configuration.setProperty(
					"hibernate.c3p0.maxStatementsPerConnection", "30");

			configuration.setProperty("hibernate.c3p0.acquire_increment", "10");

			// <property name="hibernate.show_sql">true</property>
			// <property name="hibernate.use_sql_comments">true</property>

			configuration
					.addAnnotatedClass(de.huberlin.hiwaydb.dal.Accesstime.class);

			StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
					.applySettings(configuration.getProperties());
			SessionFactory sessionFactory = configuration
					.buildSessionFactory(builder.build());

			return sessionFactory;

		} catch (Throwable ex) {
			System.err.println("Failed to create sessionFactory object." + ex);
			throw new ExceptionInInitializerError(ex);
		}

	}

}
