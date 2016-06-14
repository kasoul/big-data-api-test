package com.superh.hz.bigdata.api.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
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
	private HBaseAdmin admin = null;
	
	private HBaseClient(Configuration conf) throws IOException{
		this.conf = conf;
		this.admin = new HBaseAdmin(conf);
	}
	
	private HBaseClient() throws IOException{
		this.conf = HBaseConfiguration.create(new Configuration());
		this.admin = new HBaseAdmin(this.conf);	
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
		return admin.tableExists(table);
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
		HTableDescriptor desc = new HTableDescriptor(table);
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
		byte[] tableBytes = Bytes.toBytes(table);
		HTableDescriptor desc=admin.getTableDescriptor(tableBytes);  
	    HColumnDescriptor colDesc=new HColumnDescriptor(modifyColumnFamily);
	    desc.addFamily(colDesc);
	    admin.disableTable(tableBytes);
	    admin.modifyTable(tableBytes, desc);  
	    admin.enableTable(tableBytes); 
	}
	
	/** 使表无效*/
	public void disableTable(String table) throws IOException {
		admin.disableTable(table);
	}
	
	/** 使表生效*/
	public void enableTable(String table) throws IOException {
		admin.enableTable(table);
	}
	
	/** 判断表是否处于生效状态*/
	public boolean isTableAvailable(String table) throws IOException {
		return admin.isTableAvailable(table);
	}

	/** 删除一个表*/
	public void dropTable(String table) throws IOException {
		if (existsTable(table)) {
			disableTable(table);
			admin.deleteTable(table);
		}
	}
	
}
