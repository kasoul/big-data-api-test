package com.superh.hz.bigdata.api.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

/**
 * 数据操作方法，批量更新
 * 2015-2-3
 */
public class HBaseBatchManager {

	private Configuration configuration;
	private Connection connection;
	private BufferedMutator mutator;
	
	public HBaseBatchManager(String tableName) throws IOException{
		this(tableName,5*1024*1024,10*1024);
	}
	
	public HBaseBatchManager(String tableName,int writeBufferSize) throws IOException{
		this(tableName,writeBufferSize,10*1024);
	}
	
	public HBaseBatchManager(String tableName,int writeBufferSize,int maxKeyValueSize) throws IOException{
		this.configuration = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(configuration);
		//Admin admin = connection.getAdmin();
		//Table htable = connection.getTable(TableName.valueOf(tableName));
		BufferedMutatorParams bufferedMutatorParams = new BufferedMutatorParams(
				TableName.valueOf(tableName));
		bufferedMutatorParams.writeBufferSize(writeBufferSize);
		bufferedMutatorParams.maxKeyValueSize(maxKeyValueSize);
		mutator = connection.getBufferedMutator(
				bufferedMutatorParams);
	}
	
	public void batchPutFlush(List<Put> list){
		
		for(Put put :list){
			try {
				mutator.mutate(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void batchPut(List<Put> list){
		for(Put put :list){
			try {
				mutator.mutate(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			mutator.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void batchDeleteFlush(List<Delete> list){
		
		for(Delete delete :list){
			try {
				mutator.mutate(delete);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void flush(){
		try {
			mutator.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		try {
			mutator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		try {
			HBaseBatchManager hbm = new HBaseBatchManager("test");
			List<Put> puts = new ArrayList<>();
			Put put1 = new Put("000001".getBytes());
			put1.addColumn("cf1".getBytes(), "cq1".getBytes(), "testvalue1".getBytes());
			Put put2 = new Put("000001".getBytes());
			put2.addColumn("cf1".getBytes(), "cq2".getBytes(), "testvalue2".getBytes());
			puts.add(put1);
			puts.add(put2);
			hbm.batchPut(puts);
			hbm.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
