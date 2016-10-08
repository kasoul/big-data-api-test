package com.superh.hz.bigdata.api.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/** 
 * @Project: big-apple-api-test
 * @File: HBaseClient.java 
 * @Date: 2014年11月28日 
 * @Author: 黄超（huangchaohz）
 */
import org.apache.hadoop.hbase.util.Bytes;

/**
 *  Java clinet for HBase
 *  2015-2-3
 */
public class HBaseClient {
	
	private Configuration conf = null;
	private Admin admin = null;
	
	private HBaseClient(Configuration configuration) throws IOException{
		this.conf = configuration;
		Connection connection = ConnectionFactory.createConnection(configuration);
		this.admin = connection.getAdmin();
	}
	
	private HBaseClient() throws IOException{
		this.conf = HBaseConfiguration.create(new Configuration());
		Connection connection = ConnectionFactory.createConnection(conf);
		this.admin = connection.getAdmin();
	}
	
	public static HBaseClient getClient(Configuration conf){
		HBaseClient hbaseClient = null;
		try {
			hbaseClient =  new HBaseClient(conf);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
		return hbaseClient;
	}
	
	public static HBaseClient getClient() {
		HBaseClient hbaseClient = null;
		try {
				hbaseClient =  new HBaseClient();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
		return hbaseClient;
	}

	/** 判断表是否存在*/
	public boolean existsTable(String table) throws IOException {
		return admin.tableExists(TableName.valueOf(table));
	}

	/** 列出所有的表*/
	public String[] listTables() throws IOException {
		HTableDescriptor[] tableDescriptors = admin.listTables();
		String[] tables = new String[tableDescriptors.length];
		for(int i=0;i<tables.length;i++){
			tables[i] = tableDescriptors[i].getNameAsString();
		}
		return tables;
	}
	
	/** 创建新表*/
	public void createTable(String table, String... colfams) throws IOException {
		createTable(table, null, colfams);
	}

	/** 创建新表，初始化时根据指定的行键划分region*/
	public void createTable(String table, byte[][] splitKeys, String... colfams)
			throws IOException {
		TableName tableName = TableName.valueOf(table);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String cf : colfams) {
			HColumnDescriptor coldef = new HColumnDescriptor(cf);
			desc.addFamily(coldef);
		}
		if (splitKeys != null) {
			admin.createTable(desc, splitKeys);
		} else {
			admin.createTable(desc);
		}
	}
	
	/** 更改表结构*/
	public void modifyTable(String table, String modifyColumnFamily) throws IOException {
		TableName tableName = TableName.valueOf(table);
		HTableDescriptor desc = admin.getTableDescriptor(tableName);  
	    HColumnDescriptor colDesc=new HColumnDescriptor(modifyColumnFamily);
	    desc.addFamily(colDesc);
	    admin.disableTable(tableName);
	    admin.modifyTable(tableName, desc);  
	    admin.enableTable(tableName); 
	}
	
	/** 使表无效*/
	public void disableTable(String table) throws IOException {
		TableName tableName = TableName.valueOf(table);
		admin.disableTable(tableName);
	}
	
	/** 使表生效*/
	public void enableTable(String table) throws IOException {
		TableName tableName = TableName.valueOf(table);
		admin.enableTable(tableName);
	}
	
	/** 判断表是否处于生效状态*/
	public boolean isTableAvailable(String table) throws IOException {
		TableName tableName = TableName.valueOf(table);
		return admin.isTableAvailable(tableName);
	}

	/** 删除一个表*/
	public void dropTable(String table) throws IOException {
		TableName tableName = TableName.valueOf(table);
		if (existsTable(table)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}
	
}
