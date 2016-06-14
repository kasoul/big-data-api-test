package com.superh.hz.bigdata.api.hive.client;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/** 
 * @Project: big-apple-api-test
 * @File: HiveJDBCClient.java 
 * @Date: 2015年4月8日 
 * @Author: 黄超（huangchaohz）
 */

/**
 * @Describe:hive客户端
 */
public class HiveJDBCClient {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	/**
	 * @description
	 * @author 黄超(huangchaohz)
	 * @date 2015年4月8日
	 * @param
	 * @return
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		//System.out.println("\001");
		//testInsertTable();
		//testDeleteTableData();
		//testSelectTable();
		testCheckTableAndSchema();
	}

	/**
	 * 建表
	 */
	public static void testCreateTable() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive://node8.ssjs:10000/default", "hadoop", "");
		Statement stmt = con.createStatement();
		stmt.executeQuery("use huangchaotest");
		/*stmt.execute("drop table if exists " + tableName);
	    stmt.execute("create table " + tableName +" (id int, value string)");*/
		String createTableHql = "create table test_partition (id int,name string,no int) partitioned by (dt string)  row format delimited fields terminated by ',' stored as textfile"; 
		stmt.executeQuery(createTableHql);
		/*ResultSet res = stmt.executeQuery("desc test2" );
		while (res.next()) {
			System.out.println(res.getString(1) + " type:" + res.getString(2));
		}*/
		/*ResultSet res = stmt.executeQuery("show tables");
		while (res.next()) {
			System.out.println(res.getString(1));
		}*/
	}

	/**
	 * 查询表和库
	 */
	public static void testCheckTableAndSchema() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive://node8.ssjs:10000/huangchaotest", "hadoop", "");
		Statement stmt = con.createStatement();
		stmt.executeQuery("use huangchaotest");
		//stmt.executeQuery("drop table if exists test_external");
		//stmt.executeQuery("show tables");
		//stmt.executeQuery("drop database huangchaotest");
		ResultSet res = stmt.executeQuery("show tables");
		while (res.next()) {
			System.out.println(res.getString(1));
		}
		
		/*while (res.next()) {
			System.out.println(res.getString(1));
		}*/
	}
	

	/**
	 * 导入数据测试
	 */
	public static void testInsertTable() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive://node8.ssjs:10000/huangchaotest", "hadoop", "");
		Statement stmt = con.createStatement();
		//stmt.executeQuery("use huangchaotest");
		//从hdfs上导入数据
		String insertSql = "load data inpath '/test/hdfstesthc/test1' overwrite into table huangchaotest.test1";
		//从另一个表中导入数据
		//String insertSql = "insert overwrite table test1 select id,name,content from test2 where id=\'1\'";
		//ResultSet res = stmt.executeQuery("show databases" );
		ResultSet res = stmt.executeQuery(insertSql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}
	

	/**
	 * 查询数据测试
	 */
	public static void testSelectTable() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive://node8.ssjs:10000/test2", "hadoop", "");
		Statement stmt = con.createStatement();
		//stmt.executeQuery("use huangchaotest");
		//String selectSql = "select count(*) from test1";
		String selectSql = "select * from huangchaoTest.test2";
		ResultSet res = stmt.executeQuery(selectSql);
		/*while (res.next()) {
			System.out.println(res.getInt(1));
		}*/
		while (res.next()) {
			System.out.println("id:" + res.getString(1) + "    name:" + res.getString(2) + "    content:" + res.getString(3));
		}
	}
	
	/**
	 * 清空数据测试
	 */
	public static void testDeleteTableData() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive://node8.ssjs:10000/test2", "hadoop", "");
		Statement stmt = con.createStatement();
		stmt.executeQuery("use huangchaotest");
		String selectSql = "insert overwrite table test2 select * from test2 where 1=0";
		ResultSet res = stmt.executeQuery(selectSql);
		while (res.next()) {
			System.out.println(res.getInt(1));
		}
	}
	
}
