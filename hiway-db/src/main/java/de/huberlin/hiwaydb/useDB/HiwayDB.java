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
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
		List<T> r = new ArrayList<>(c.size());
		for (Object o : c)
			r.add(clazz.cast(o));
		return r;
	}

	private String configFile = "hibernate.cfg.xml";

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

			dbVolume = (long) session.createCriteria(Workflowrun.class).setProjection(Projections.rowCount()).uniqueResult();

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
	}

	@Override
	public Set<String> getHostNames() {
		Long tick = System.currentTimeMillis();
		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}

		Set<String> tempResult = new HashSet<>();

		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		Session sess = dbSessionFactory.openSession();

		// Non-managed environment idiom with getCurrentSession()
		try {

			txMessung = sessAT.beginTransaction();
			Accesstime at = new Accesstime();

			at.setTick(tick);
			at.setFunktion("getHostNames");
			at.setInput("SQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);

			Query query = null;

			String hql = "SELECT I.hostname FROM Invocation I where I.hostname!=null";
			query = sess.createQuery(hql);

			for (Object i : query.list()) {
				tempResult.add((String) i);
			}

			Long x = (long) tempResult.size();

			at.setReturnvolume(x);
			Long tock = System.currentTimeMillis();
			at.setTock(tock);
			at.setTicktockdif(tock - tick);
			at.setRunId(this.runIDat);
			at.setWfName(this.wfName);
			sessAT.save(at);
			txMessung.commit();
		} catch (RuntimeException e) {
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

	@Override
	public Collection<InvocStat> getLogEntriesForTasks(Set<Long> taskIds) {
		Long tick = System.currentTimeMillis();

		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}
		List<Invocation> resultsInvoc = new ArrayList<>();
		Collection<InvocStat> resultList;
		Session sess = dbSessionFactory.openSession();
		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;

		Accesstime at = new Accesstime();

		try {
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

			query = sess.createQuery(queryString.substring(0, queryString.length() - 4));

			// join I.invocationId
			resultsInvoc = castList(Invocation.class, query.list());
			Long x = (long) resultsInvoc.size();

			at.setReturnvolume(x);
			resultList = createInvocStat(resultsInvoc, null);

		} catch (RuntimeException e) {
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

		Set<Long> tempResult = new HashSet<>();

		Session sess = dbSessionFactory.openSession();
		Session sessAT = dbSessionFactoryMessung.openSession();

		Accesstime at = new Accesstime();

		Transaction txMessung = null;
		try {
			txMessung = sessAT.beginTransaction();

			at.setTick(tick);
			at.setFunktion("getTaskIdsForWorkflow");
			at.setInput("SQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);

			Query query = null;

			query = sess.createQuery("FROM Workflowrun W WHERE W.wfname ='" + workflowName + "'");

			for (Object w : query.list()) {
				for (Invocation i : ((Workflowrun) w).getInvocations()) {
					tempResult.add(i.getTask().getTaskId());
				}
			}

			Long x = (long) tempResult.size();

			at.setReturnvolume(x);

			// tx.commit();

		} catch (RuntimeException e) {
			// if (tx != null)
			// tx.rollback();
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

		Session sess = dbSessionFactory.openSession();
		// Transaction tx = null;
		String result = "";
		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		Accesstime at = new Accesstime();

		try {
			// tx = sess.beginTransaction();
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

			Long x = (long) query.list().size();

			at.setReturnvolume(x);

			if (!query.list().isEmpty()) {
				result = ((Task) query.list().get(0)).getTaskName();
			}

		} catch (RuntimeException e) {
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

	private static Collection<InvocStat> createInvocStat(List<Invocation> invocations, Session sess) {

		Set<InvocStat> resultList = new HashSet<>();
		Invocation tempInvoc;

		for (int i = 0; i < invocations.size(); i++) {
			tempInvoc = invocations.get(i);

			InvocStat invoc = new InvocStat(tempInvoc.getWorkflowrun().getRunId(), tempInvoc.getTask().getTaskId());

			if (tempInvoc.getHostname() != null && tempInvoc.getTask().getTaskId() != 0 && tempInvoc.getRealTime() != null) {
				invoc.setHostName(tempInvoc.getHostname());
				invoc.setRealTime(tempInvoc.getRealTime(), tempInvoc.getTimestamp());

				Set<FileStat> iFiles = new HashSet<>();
				Set<FileStat> oFiles = new HashSet<>();

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

				resultList.add(invoc);
			}
		}
		if (sess != null && sess.isOpen()) {
			sess.close();

			System.out.println("hiwayDB | Close Session  -> CreateInvocStat DONE ->Size: " + resultList.size());
		}

		return resultList;
	}

	@Override
	public Collection<InvocStat> getLogEntriesForTaskOnHostSince(long taskId, String hostName, long timestamp) {
		Long tick = System.currentTimeMillis();
		if (dbSessionFactory == null) {
			dbSessionFactory = getSQLSession();
		}

		List<Invocation> resultsInvoc = new ArrayList<>();
		Collection<InvocStat> resultList;

		Session sess = dbSessionFactory.openSession();
		Session sessAT = dbSessionFactoryMessung.openSession();
		Transaction txMessung = null;
		Accesstime at = new Accesstime();

		try {
			txMessung = sessAT.beginTransaction();

			at.setTick(tick);
			at.setFunktion("getLogEntriesForTaskOnHostSince");
			at.setInput("SQL");
			at.setConfig(config);
			at.setDbvolume(dbVolume);

			Query query = null;

			query = sess.createQuery("FROM Invocation I  WHERE I.hostname ='" + hostName + "' and I.Timestamp >" + timestamp + " and I.task = " + taskId);

			resultsInvoc = castList(Invocation.class, query.list());

			Long x = (long) resultsInvoc.size();

			at.setReturnvolume(x);

			resultList = createInvocStat(resultsInvoc, null);

		} catch (RuntimeException e) {
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

			System.out.println("hiwayDB | start adding Entry time: " + tick);

			tx = oneSession.beginTransaction();
			txMessung = sessAT.beginTransaction();

			Accesstime at = new Accesstime();

			at.setTick(tick);
			at.setFunktion("JsonReportEntryToDB");
			at.setInput("SQL");
			at.setDbvolume(dbVolume);

			Query query = null;

			String runID = null;
			Long wfId = null;

			Workflowrun wfRun = null;
			if (logEntryRow.getRunId() != null) {
				runID = logEntryRow.getRunId().toString();

				query = oneSession.createQuery("FROM Workflowrun E WHERE E.runid='" + runID + "'");

				if (query.list() != null && !query.list().isEmpty()) {

					wfRun = (Workflowrun) query.list().get(0);
					wfId = wfRun.getId();
				}
			}

			long taskID = 0;
			Task task = null;
			if (logEntryRow.getTaskId() != null) {
				taskID = logEntryRow.getTaskId();

				query = oneSession.createQuery("FROM Task E WHERE E.taskid =" + taskID);
				if (query.list() != null && !query.list().isEmpty()) {
					task = (Task) query.list().get(0);
				}
			}

			Long invocID = (long) 0;

			if (logEntryRow.hasInvocId()) {
				invocID = logEntryRow.getInvocId();
				System.out.println("Has InvovID: " + invocID);
			}

			query = oneSession.createQuery("FROM Invocation E WHERE E.invocationid =" + invocID + " and E.workflowrun='" + wfId + "'");
			List<Invocation> resultsInvoc = castList(Invocation.class, query.list());

			Long timestampTemp = logEntryRow.getTimestamp();

			Invocation invoc = null;
			if (resultsInvoc != null && !resultsInvoc.isEmpty()) {
				invoc = resultsInvoc.get(0);
				System.out.println("invoc gefunden: ID " + invoc.getInvocationId());
			}

			if (wfRun == null && runID != null) {

				wfRun = new Workflowrun();
				wfRun.setRunId(runID);
				oneSession.save(wfRun);
				System.out.println("hiwayDB | save WfRun: " + runID);
				this.runIDat = runID;
			}

			if (taskID != 0 && (task == null)) {
				task = new Task();

				task.setTaskId(taskID);
				task.setTaskName(logEntryRow.getTaskName());
				task.setLanguage(logEntryRow.getLang());

				oneSession.save(task);
				System.out.println("hiwayDB | save Task: " + taskID + " - " + task.getTaskName());
			}

			if (invocID != 0 && (invoc == null)) {
				invoc = new Invocation();
				invoc.setTimestamp(timestampTemp);
				invoc.setInvocationId(invocID);
				invoc.setTask(task);
				invoc.setWorkflowrun(wfRun);
				oneSession.save(invoc);
				System.out.println("hiwayDB | save Invoc: " + invocID);
			}

			String filename = null;

			File file = null;
			if (logEntryRow.getFile() != null && invoc != null) {
				filename = logEntryRow.getFile();

				query = oneSession.createQuery("FROM File E WHERE E.name='" + filename + "' AND E.invocation=" + invoc.getId());

				System.out.println("File Query:" + query.toString());
				if (query.list() != null && !query.list().isEmpty()) {
					file = (File) query.list().get(0);
					System.out.println("File haben wir:" + file.getName() + file.getId());
				}
			}

			if (file == null && filename != null) {

				file = new File();
				file.setName(filename);
				file.setInvocation(invoc);
				oneSession.save(file);
				System.out.println("hiwayDB | save File: " + filename);
			}

			String key = logEntryRow.getKey();

			System.out.println("hiwayDB | save KEY: " + key);

			at.setKeyinput(key);

			JSONObject valuePart;
			switch (key) {
			case HiwayDBI.KEY_INVOC_HOST:
				if (invoc != null)
					invoc.setHostname(logEntryRow.getValueRawString());
				break;
			case "wf-name":
				if (wfRun != null)
					wfRun.setWfName(logEntryRow.getValueRawString());
				this.wfName = logEntryRow.getValueRawString();
				break;
			case "wf-time":
				String val = logEntryRow.getValueRawString();
				Long test = Long.parseLong(val, 10);
				if (wfRun != null)
					wfRun.setWfTime(test);
				break;
			case HiwayDBI.KEY_INVOC_TIME_SCHED:
				valuePart = logEntryRow.getValueJsonObj();
				if (invoc != null)
					invoc.setScheduleTime(GetTimeStat(valuePart));
				break;
			case JsonReportEntry.KEY_INVOC_STDERR:
				if (invoc != null)
					invoc.setStandardError(logEntryRow.getValueRawString());
				break;
			case JsonReportEntry.KEY_INVOC_SCRIPT:
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
				if (invoc != null)
					invoc.setStandardOut(logEntryRow.getValueRawString());
				break;
			case "invoc-time-stagein":
				valuePart = logEntryRow.getValueJsonObj();
				if (invoc != null)
					invoc.setRealTimeIn(GetTimeStat(valuePart));
				break;
			case "invoc-time-stageout":
				valuePart = logEntryRow.getValueJsonObj();
				if (invoc != null)
					invoc.setRealTimeOut(GetTimeStat(valuePart));
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEIN:
				valuePart = logEntryRow.getValueJsonObj();
				if (file != null)
					file.setRealTimeIn(GetTimeStat(valuePart));
				break;
			case HiwayDBI.KEY_FILE_TIME_STAGEOUT:
				valuePart = logEntryRow.getValueJsonObj();
				if (file != null)
					file.setRealTimeOut(GetTimeStat(valuePart));
				break;

			case JsonReportEntry.KEY_INVOC_TIME:
				valuePart = logEntryRow.getValueJsonObj();
				try {
					if (invoc != null)
						invoc.setRealTime(GetTimeStat(valuePart));
				} catch (NumberFormatException e) {
					if (invoc != null)
						invoc.setRealTime(1l);
				}

				break;
			case "file-size-stagein":
				if (file != null)
					file.setSize(Long.parseLong(logEntryRow.getValueRawString(), 10));
				break;
			case "file-size-stageout":
				if (file != null)
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
			default:
			}

			tx.commit();

			at.setReturnvolume((long) logEntryRow.toString().length());
			Long tock = System.currentTimeMillis();
			at.setTock(tock);
			at.setTicktockdif(tock - tick);
			at.setWfName(this.wfName);
			at.setRunId(this.runIDat);

			sessAT.save(at);

			txMessung.commit();

		} catch (org.hibernate.exception.ConstraintViolationException e) {

			System.out.println("hiwayDB FEHLER | ConstraintViolationException: " + e.getConstraintName());
			String message = e.getSQLException().getMessage();

			if (message.contains("RundID_UNIQUE")) {
				System.out.println("hiwayDB FEHLER | runIDUnique");
			} else if ((message.contains("JustOneFile"))) {
				System.out.println("hiwayDB FEHLER | JustOneFile");
			}
			logStackTrace(e);

			if (tx != null) {
				System.out.println("hiwayDB Rollback");
				tx.rollback();
			}
			System.exit(1);

		} catch (Exception e) {
			if (tx != null) {
				System.out.println("hiwayDB Rollback");
				tx.rollback();
			}
			System.out.println("hiwayDB FEHLER | " + e);

			logStackTrace(e);
			System.exit(1);

		} finally {
			if (oneSession.isOpen()) {
				oneSession.close();
			}
			if (sessAT.isOpen()) {
				sessAT.close();
			}
		}
	}

	private static Long GetTimeStat(JSONObject valuePart) {
		return Long.parseLong(valuePart.get("realTime").toString(), 10);
	}

	private SessionFactory getSQLSession() {
		try {

			if (dbURL != null && username != null) {

				Configuration configuration = new Configuration();

				configuration.setProperty("hibernate.connection.url", dbURL);
				configuration.setProperty("hibernate.connection.username", username);
				if (this.password != null) {
					configuration.setProperty("hibernate.connection.password", this.password);
				} else {
					configuration.setProperty("hibernate.connection.password", "");
				}

				configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLInnoDBDialect");
				configuration.setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver");

				configuration.setProperty("connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider");

				configuration.setProperty("hibernate.transaction.factory_class", "org.hibernate.transaction.JDBCTransactionFactory");

				configuration.setProperty("hibernate.current_session_context_class", "thread");

				configuration.setProperty("hibernate.initialPoolSize", "20");
				configuration.setProperty("hibernate.c3p0.min_size", "5");
				configuration.setProperty("hibernate.c3p0.max_size", "1000");

				configuration.setProperty("hibernate.maxIdleTime", "3600");
				configuration.setProperty("hibernate.c3p0.maxIdleTimeExcessConnections", "300");

				configuration.setProperty("hibernate.c3p0.timeout", "330");
				configuration.setProperty("hibernate.c3p0.idle_test_period", "300");

				configuration.setProperty("hibernate.c3p0.max_statements", "13000");
				configuration.setProperty("hibernate.c3p0.maxStatementsPerConnection", "30");

				configuration.setProperty("hibernate.c3p0.acquire_increment", "10");

				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Hiwayevent.class);
				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.File.class);
				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Inoutput.class);
				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Invocation.class);
				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Task.class);
				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Userevent.class);
				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Workflowrun.class);
				configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Accesstime.class);

				StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties());
				SessionFactory sessionFactory = configuration.buildSessionFactory(builder.build());
				System.out.println("Session Factory Starten!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NEUE Tocks gesetzt");
				return sessionFactory;

			}

			java.io.File f = new java.io.File(configFile);

			Configuration configuration = new Configuration().configure(f);
			StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties());
			SessionFactory sessionFactory = configuration.buildSessionFactory(builder.build());
			return sessionFactory;

		} catch (Throwable ex) {
			System.err.println("Failed to create sessionFactory object." + ex);
			throw new ExceptionInInitializerError(ex);
		}

	}

	public static void logStackTrace(Throwable e) {
		Writer writer = new StringWriter();
		PrintWriter printWriter = new PrintWriter(writer);
		e.printStackTrace(printWriter);
		System.err.println(writer.toString());
	}

	private SessionFactory getSQLSessionMessung() {
		try {

			String url = dbURL.substring(0, dbURL.lastIndexOf("/")) + "/messungen";

			Configuration configuration = new Configuration();

			configuration.setProperty("hibernate.connection.url", url);
			configuration.setProperty("hibernate.connection.username", username);
			if (this.password != null) {
				configuration.setProperty("hibernate.connection.password", this.password);
			} else {
				configuration.setProperty("hibernate.connection.password", "");
			}

			configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLInnoDBDialect");
			configuration.setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver");

			configuration.setProperty("connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider");

			configuration.setProperty("hibernate.transaction.factory_class", "org.hibernate.transaction.JDBCTransactionFactory");

			configuration.setProperty("hibernate.current_session_context_class", "thread");

			configuration.setProperty("hibernate.initialPoolSize", "20");
			configuration.setProperty("hibernate.c3p0.min_size", "5");
			configuration.setProperty("hibernate.c3p0.max_size", "1000");

			configuration.setProperty("hibernate.maxIdleTime", "3600");
			configuration.setProperty("hibernate.c3p0.maxIdleTimeExcessConnections", "300");

			configuration.setProperty("hibernate.c3p0.timeout", "330");
			configuration.setProperty("hibernate.c3p0.idle_test_period", "300");

			configuration.setProperty("hibernate.c3p0.max_statements", "13000");
			configuration.setProperty("hibernate.c3p0.maxStatementsPerConnection", "30");

			configuration.setProperty("hibernate.c3p0.acquire_increment", "10");

			configuration.addAnnotatedClass(de.huberlin.hiwaydb.dal.Accesstime.class);

			StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties());
			SessionFactory sessionFactory = configuration.buildSessionFactory(builder.build());

			return sessionFactory;

		} catch (Throwable ex) {
			System.err.println("Failed to create sessionFactory object." + ex);
			throw new ExceptionInInitializerError(ex);
		}

	}

}
