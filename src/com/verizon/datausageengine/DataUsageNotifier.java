package com.verizon.datausageengine;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class DataUsageNotifier {

	public static void main(String[] args) {
		Cluster cluster = null;
		Session session = null;
		try {

			ArrayList distictCustid = new ArrayList();
			HashMap custIdCump = new HashMap();
			// Connect to the cluster and keyspace "DATA_USAGE"
			cluster = Cluster.builder().addContactPoint("113.128.163.213").build();
			session = cluster.connect("DATA_USAGE");
			/*
			 * ResultSet results = session.execute(
			 * "select system.sum(usage)as usage, customerid from test.cust where starttime >'2015-10-01 00:00:00'  and customerid='0004' ALLOW FILTERING"
			 * ); for (Row row : results) { System.out.format("" +
			 * row.getLong("usage") + row.getString("customerid")); // Clean up
			 * the connection by closing it }
			 */

			distictCustid = getalltheUniqueCustomer(session);
			custIdCump = getActualUsage(session, distictCustid);
			insertCusmTable(session, custIdCump);
//			alertCumption(22, 0, session);
//			Date starttime = null;
//			Date endtime = null;
//			CumptionbyTime(session, custIdCump, starttime, endtime);
		} finally {
			session.close();
			cluster.close();

		}
	}

	private static long CumptionbyTime(Session sess,HashMap custIdCump, Date starttime, Date endtime) {
		
		String query="select System.sum(usage) as usage from DATA_USAGE.CONSUMPTION where startTime='"+starttime+"' and endTime='"+endtime+"' ALLOW FILTERING";
		ResultSet rs=sess.execute(query);
		for(Row row:rs){
			long usage= row.getLong("usage");
			return usage;
		}
		return 0;
		
	}

	private static void updateCumption(Session sess, String custIdCump, int i) {
		int newal = i + 1;
		sess.execute("update user_profile set no_of_Notification=" + newal + " where customerId='" + custIdCump + "'");
	}

	private static void alertCumption(int range, int noofAlert, Session sess) {
		String custIdcump = null;
		String query = "select customerId, no_of_notification from DATA_USAGE.USER_PROFILE where perconsup = " + range
				+ " and no_of_notification = " + noofAlert + " ALLOW FILTERING";
		System.out.println("final " + query);
		ResultSet rs1 = sess.execute(query);
		for (Row row : rs1) {
			custIdcump = row.getString("customerId");
			System.out.println("Alert this customer:::" + custIdcump + "::;");
			updateCumption(sess, custIdcump, noofAlert);
		}

	}

	private static void insertCusmTable(Session sess, HashMap custIdCump) {
		System.out.println("am here");
		int finalNotification= 0;
//		custIdCump.put("007007007", "95");
//		sess.execute("CREATE TABLE DATA_USAGE.USER_PROFILE(CustomerID text, PerConsup int, no_of_Notification int, PRIMARY KEY(Customerid))");
		Set<String> keys = custIdCump.keySet();
		for (String key : keys) {
			System.out.println(key);
			System.out.println("select no_of_Notification from DATA_USAGE.USER_PROFILE where customerId = '"+key+"'");
			ResultSet rs1 = sess.execute("select no_of_Notification from DATA_USAGE.USER_PROFILE where customerId = '"+key+"'");
			for(Row rw1 : rs1){
				finalNotification = rw1.getInt("no_of_Notification");
				System.out.println("The insert is complete");
			}
			System.out.println("INSERT INTO DATA_USAGE.USER_PROFILE (CustomerID , perconsup,no_of_notification)VALUES ("
					+ "'" + key + "'" + "," + custIdCump.get(key) + "," + finalNotification + ")");
			sess.execute("INSERT INTO DATA_USAGE.USER_PROFILE (CustomerID , perconsup,no_of_notification)VALUES (" + "'"
					+ key + "'" + "," + custIdCump.get(key) + "," + finalNotification + ")");
		}

	}

	private static HashMap getActualUsage(Session sess, ArrayList mylist) {
		HashMap tmp = new HashMap();
		int tmp1 = 0;
		for (int i = 0; i < mylist.size(); i++) {
			String custid = (String) mylist.get(i);
			ResultSet rs = sess.execute(
					"Select System.sum(usage) as usage,customerId from DATA_USAGE.CONSUMPTION where customerId=" + "'"
							+ custid.toString() + "'" + " and starttime < dateof(now())");
			for (Row row : rs) {
				Long usage = row.getLong("usage"); 
				int finalUsage = (int) getPercentage(usage);
				System.out.println("the Current Usage :::: "+finalUsage+"::::");
				
				// Start New Changes for the free sites
//				long freeSiteUsage =0;
				int freeusageinGB =0;
				ResultSet rs2= sess.execute("Select site from DATA_USAGE.FREESITES");
				
				for(Row rw1: rs2){
					String site = rw1.getString("site");
					System.out.println("Select System.sum(usage) as usage,customerId from DATA_USAGE.CONSUMPTION where customerId=" + "'"
							+ custid.toString() + "'" + " and starttime < dateof(now()) and website='"+site+"'");
					ResultSet rs3 = sess.execute("Select System.sum(usage) as usage,customerId from DATA_USAGE.CONSUMPTION where customerId=" + "'"
							+ custid.toString() + "'" + " and starttime < dateof(now()) and website='"+site+"'");
					for(Row rw3:rs3){
						System.out.println("free site found ::::"+custid.toString()+":::");
						long freeSiteUsage = rw3.getLong("usage");
						System.out.println("The freeSiteUsage in byte:::"+freeSiteUsage+"::::");
					}
					
				}
				finalUsage = finalUsage - freeusageinGB;
				
				System.out.println("the Current Usage :::: "+finalUsage+"::::");
				System.out.println("the Free Usage :::: "+freeusageinGB+"::::");
				System.out.println("the Billable Usage :::: "+finalUsage+"::::");
				// End New Changes for the free sites
				
				if (finalUsage >= 95) {
					tmp1 = 95;
				} else if (finalUsage >= 90) {
					tmp1 = 90;
				} else if (finalUsage >= 85) {
					tmp1 = 85;
				} else if (finalUsage >= 80) {
					tmp1 = 80;
				} else if (finalUsage >= 75) {
					tmp1 = 75;
				} else if (finalUsage >= 70) {
					tmp1 = 70;
				} else {
					tmp1 = finalUsage;
				}

				tmp.put(custid, tmp1);
			}
		}
		return tmp;
	}

	private static float getPercentage(Long usage) {
		int size = 1024;
		int value = (int) (usage / (size * size * size));
//		System.out.println("Usage:::" + usage + "+:Percentage is" + value);
		return value;
	}

	private static ArrayList getalltheUniqueCustomer(Session sess) {
		ArrayList tmp = new ArrayList();

		ResultSet rs = sess.execute("Select distinct customerid From DATA_USAGE.CONSUMPTION");
		for (Row row : rs) {
			System.out.println("The distict custId is ::::" + row.getString("customerid") + "::::");
			tmp.add(row.getString("customerid"));
		}
		return tmp;

	}

}
