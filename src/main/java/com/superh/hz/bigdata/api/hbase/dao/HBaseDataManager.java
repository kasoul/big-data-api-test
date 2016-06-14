package com.superh.hz.bigdata.api.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
/*import org.apache.hadoop.hbase.client.RowLock;*/
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.superh.hz.bigdata.api.hbase.tool.userFilter.HCTestFilter;

/** 
 * @Project: big-apple-api-test
 * @File: HBaseDataManager.java 
 * @Date: 2014年11月29日 
 * @Author: 黄超（huangchaohz）
 */

/**
 * @Describe:数据操作方法，默认的rowkey是String类型
 */
public class HBaseDataManager {

	private HTableInterface table;
	private String tableName;

	private HBaseDataManager(String tableName) throws IOException{
		this.tableName = tableName;
		table = new HTable(HBaseConfiguration.create(new Configuration()),tableName);
	}
	
	/*private HBaseDataManager(String tableName,Configuration configuration ) throws IOException{
		//这个是最好的方法
		HConnection connection = HConnectionManager.createConnection(configuration);
		table = connection.getTable(tableName);
		table2 = connection.getTable(tableName);
		table3 = connection.getTable(tableName);
	}*/
	
	private HBaseDataManager(String tableName,Configuration configuration ) throws IOException{
		//这个是最好的方法
		HConnection connection = HConnectionManager.createConnection(configuration);
		table = connection.getTable(tableName);
		/*table2 = connection.getTable(tableName);
		table3 = connection.getTable(tableName);*/
	}
	
	public static HBaseDataManager getHBaseDataManager(String tableName){
		HBaseDataManager hbaseDataManager = null;
		try {
			hbaseDataManager = new HBaseDataManager(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hbaseDataManager;
	}
	
	public void close(){
		try {
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setAutoFlush(boolean flag){
		table.setAutoFlushTo(flag);
		//table.setWriteBufferSize(10*1024*1024);//超过设置的字节数byte数，自动提交到服务器，否则在客户端缓存不提交
	}
	
	public void setWriteBufferSize(long mem){
		try {
			table.setWriteBufferSize(mem);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void flushCommits(){
		try {
			table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}
	
	/** 插入一个cell 的value*/
	public void insertData(String rowkey, String columnFamily, String columnQualify, byte[] value) {
		Put put = new Put(rowkey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		put.add(columnFamily.getBytes(), columnQualify.getBytes(), value);// 本行数据的第一列
		//put.add(family, qualifier, ts, value) 插入带有时间戳的cell值
		
		try {
			table.put(put);
			System.out.println("Insert data into table:" + tableName +" on row:" + rowkey );
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/** 插入多个put，批量插入*/
	public void insertDataBatch(List<Put> puts) {
		try {
			table.put(puts);
			//System.out.println("Insert Mutiple data into table:" + tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/** 删除一个rowkey*/
	public void deleteData(String rowkey) {
	 try { 
         List<Delete> deleteList = new ArrayList<Delete>(); 
         Delete delete = new Delete(rowkey.getBytes()); 
         //Delete delete = new Delete(row, timestamp) 删除比这个时间戳小或相等的所有版本的cell
         deleteList.add(delete); 
         table.delete(deleteList); 
         System.out.println("delete row:" + rowkey + " in table:" + tableName); 
     } catch (IOException e) { 
         e.printStackTrace(); 
     }
	}
	
	/** 删除多行*/
	public void deleteMultiRow(List<Delete> deleteList) {
		 try { 
	         table.delete(deleteList); 
	         System.out.println("delete mutil rows in table:" + tableName); 
	     } catch (IOException e) { 
	         e.printStackTrace(); 
	     } 
	}
	
	/** 获取所有数据*/
	public Set<String> queryAll() { 
		 Set<String> rowkeys = new HashSet<String>();
	     try { 
	    	 Scan scan = new Scan();
	    	 scan.setCacheBlocks(false);//CacheBlock默认为true，如果列族属性为不支持块缓存，则CacheBlock永远不开启
	    	 scan.setCaching(500000); //每次访问服务器获取的记录数量，非常重要
	    	 // scan.setMaxResultSize(10*1024*1024);
	    	 // scan.addColumn("label".getBytes(), "gender".getBytes());
	    	 // scan.setStopRow("123456999".getBytes());
	    	 // scan.setStartRow(startRow);//设置查询的最小rowkey，结果包括startRow
	    	 // scan.setStopRow(stopRow)；设置查询的最大rowkey，结果不包括stopRow
	    	 // scan.setMaxVersions();查询出所有版本的cell，按时间戳降序排列,如果这个不设置，只查询出一条最新版本的cell
	    	 // scan.setMaxVersions(maxVersions);查询的版本数量
	    	 // scan.setTimeRange(minStamp, maxStamp);
	    	 // 查询TIMERANGE在minStamp和maxStamp之间的cell，包含minStamp但是不包含maxStamp。查询出的是时间戳最大也就是最新的一个版本。
	    	 // scan.setTimeStamp(timestamp);查询时间戳刚好等于给定timestamp的版本
	         ResultScanner rs = table.getScanner(scan);
	         int count=0;
	         for (Result r : rs) { 
	        	 count++;
	        	 //String row = new String(r.getRow());
	        	 rowkeys.add(new String(r.getRow()));
	          /* System.out.println("获得到rowkey:" + new String(r.getRow())); 
	             for (KeyValue keyValue : r.raw()) { 
	                	  System.out.println("--------------------" + new String(keyValue.getRow()) + "----------------------------");
	                      System.out.println("Column Family: " + new String(keyValue.getFamily()));
	                      System.out.println("Column Qualify      :" + new String(keyValue.getQualifier()));
	                      System.out.println("Value        : " + Bytes.toString(keyValue.getValue())); 
	                      System.out.println("Timestamp        : " + keyValue.getTimestamp()); 
	              } */
	         } 
	         System.out.println(count);
	     } catch (IOException e) { 
	        e.printStackTrace(); 
	     } 
	     return rowkeys;
	} 
	
	/** 获取某行的数据*/
	public void queryByRowkey(String rowkey) { 
        try { 
            Get get = new Get(rowkey.getBytes());// 根据rowkey查询
           // get.setTimeRange(minStamp, maxStamp);
           // 查询TIMERANGE在minStamp和maxStamp之间的cell，包含minStamp但是不包含maxStamp。查询出的是时间戳最大也就是最新的一个版本。
           // get.setTimeStamp(timestamp);查询时间戳刚好等于给定timestamp的版本
            Result r = table.get(get); //result代表一行的数据，实际上是一个cell的集合
            System.out.println("获得到rowkey:" + new String(r.getRow())); 
            for (KeyValue keyValue : r.raw()) { //KeyValue是一个cell，按family，qualify排序
                System.out.println("--------------------" + new String(keyValue.getRow()) + "----------------------------");
                System.out.println("Column Family: " + new String(keyValue.getFamily()));
                System.out.println("Column Qualify      :" + new String(keyValue.getQualifier()));
                System.out.println("Value        : " + new String(keyValue.getValue()));
                System.out.println("Timestamp        : " + keyValue.getTimestamp()); 
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    }

	/** 获取某个cell的数据*/
	public void queryByRowkeyAndQualify(String rowkey,String columnFamily,String columnQualify) { 
        try { 
            Get get = new Get(rowkey.getBytes());// 根据rowkey查询
           // get.addFamily(columnFamily.getBytes());
            get.addColumn(columnFamily.getBytes(), columnQualify.getBytes());
            Result r = table.get(get); 
            System.out.println("获得到rowkey:" + new String(r.getRow())); 
            for (KeyValue keyValue : r.raw()) { //KeyValue是一个cell，按family，qualify排序
                System.out.println("--------------------" + new String(keyValue.getRow()) + "----------------------------");
                System.out.println("Column Family : " + new String(keyValue.getFamily()));
                System.out.println("Column Qualify:" + new String(keyValue.getQualifier()));
                System.out.println("Value         : " + Bytes.toString(keyValue.getValue()));
                System.out.println("Timestamp     : " + keyValue.getTimestamp());
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    }
	
	/** 获取某个cell的数据*/
	public void queryByRowkeyAndQualify(List<String> rowkeys,String columnFamily,String columnQualify) { 
        try { 
        	List<Get> gets = new ArrayList<Get>();
        	for(String rowkey:rowkeys){
        		Get get = new Get(rowkey.getBytes());// 根据rowkey查询
        		// get.addFamily(columnFamily.getBytes());
        		get.addColumn(columnFamily.getBytes(), columnQualify.getBytes());
        		gets.add(get);
        	}
            Result[] results = table.get(gets); 
            for(Result r:results){
            System.out.println("获得到rowkey:" + new String(r.getRow())); 
            	for (KeyValue keyValue : r.raw()) { //KeyValue是一个cell，按family，qualify排序
            		System.out.println("--------------------" + new String(keyValue.getRow()) + "----------------------------");
            		System.out.println("Column Family : " + new String(keyValue.getFamily()));
            		System.out.println("Column Qualify:" + new String(keyValue.getQualifier()));
            		System.out.println("Value         : " + Bytes.toString(keyValue.getValue()));
            		System.out.println("Timestamp     : " + keyValue.getTimestamp());
            	} 
            }
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    }
	
	/** 通过单个Filter查询数据*/
	public void queryByFilter(String columnFamily, String coulmnQuaify, String value) { 
	     try { 
	           /* Filter filter = new SingleColumnValueFilter(Bytes 
	                    .toBytes(columnFamily), Bytes 
	                    .toBytes(coulmnQuaify), CompareOp.EQUAL, Bytes 
	                    .toBytes(value)); */
	    	 /*//region个数 * pageSize
	 			Filter pageFilter = new PageFilter(1);
	 			scan.setFilter(pageFilter);*/
	    	 Filter filter  = new HCTestFilter();
	            Scan scan = new Scan(); 
	            scan.setCaching(2500);
	            scan.setFilter(filter); 
	            ResultScanner rs = table.getScanner(scan); 
	            /*根据filter取出的是整个row，
	             * row里其他的column family和column qualify也会被取出来
	             * 如果一个row里不包含该coulmnQuaify，也会被查询出来
	             * 所以filter查询是不准确的查询*/
	            for (Result r : rs){ 
	                System.out.println("获得到rowkey:" + new String(r.getRow())); 
	                for (KeyValue keyValue : r.raw()) { 
	                	  System.out.println("--------------------" + new String(keyValue.getRow()) + "----------------------------");
	                      System.out.println("Column Family : " + new String(keyValue.getFamily()));
	                      System.out.println("Column Qualify:" + new String(keyValue.getQualifier()));
	                      System.out.println("Value         : " + Bytes.toString(keyValue.getValue())); 
	                      System.out.println("Timestamp        : " + keyValue.getTimestamp()); 
	                } 
	            } 
	            rs.close();
	        } catch (Exception e) { 
	            e.printStackTrace(); 
	        }
	 } 
	
	/** 通过单个Filter查询数据*/
	public void queryByParamterFilter(Filter filter) { 
	     try { 
	            Scan scan = new Scan(); 
	            scan.setCaching(2500);
	            scan.setFilter(filter); 
	            scan.setStartRow(Bytes.toBytes("12347678909"));
	            scan.setReversed(true);
	            ResultScanner rs = table.getScanner(scan); 
	            /*根据filter取出的是整个row，
	             * row里其他的column family和column qualify也会被取出来
	             * 如果一个row里不包含该coulmnQuaify，也会被查询出来
	             * 所以filter查询是不准确的查询*/
	            int count = 0;
	            for (Result r : rs){ 
	            	count++;
	            	if(count>=10){
	            		break;
	            	}
	                System.out.println("获得到rowkey:" + new String(r.getRow())); 
	                for (KeyValue keyValue : r.raw()) { 
	                	  System.out.println("--------------------" + new String(keyValue.getRow()) + "----------------------------");
	                      System.out.println("Column Family : " + new String(keyValue.getFamily()));
	                      System.out.println("Column Qualify:" + new String(keyValue.getQualifier()));
	                      System.out.println("Value         : " + new String(keyValue.getValue(),"GBK")); 
	                      System.out.println("Timestamp        : " + keyValue.getTimestamp()); 
	                } 
	            } 
	            rs.close();
	        } catch (Exception e) { 
	            e.printStackTrace(); 
	        }
	 } 
	
	 /** 通过多个Filter查询数据*/
	 public void queryByFilterList() { 
	     try { 
	 
	            List<Filter> filters = new ArrayList<Filter>(); 
	 
	            Filter filter1 = new SingleColumnValueFilter(Bytes 
	                    .toBytes("columnFamily1"), Bytes 
	                    .toBytes("columnQualify1"), CompareOp.EQUAL, Bytes 
	                    .toBytes("value1")); 
	            filters.add(filter1); 
	 
	            Filter filter2 = new SingleColumnValueFilter(Bytes 
	                    .toBytes("columnFamily2"), null, CompareOp.EQUAL, Bytes 
	                    .toBytes("value2")); 
	            filters.add(filter2); 
	 
	            Filter filter3 = new SingleColumnValueFilter(Bytes 
	                    .toBytes("columnFamily3"), null, CompareOp.EQUAL, Bytes 
	                    .toBytes("value3")); 
	            filters.add(filter3); 
	 
	            FilterList filterList = new FilterList(filters); 
	            Scan scan = new Scan(); 
	            scan.setFilter(filterList); 
	            ResultScanner rs = table.getScanner(scan); 
	            for (Result r : rs) { 
	                System.out.println("获得到rowkey:" + new String(r.getRow())); 
	                for (KeyValue keyValue : r.raw()) { 
	                    System.out.println("列：" + new String(keyValue.getFamily())  + new String(keyValue.getQualifier())
	                            + "====值:" + new String(keyValue.getValue())); 
	                }
	            }
	            rs.close();
	        } catch (Exception e) { 
	            e.printStackTrace(); 
	        } 
	 }
	 
	 /** 对某个cell自增*/
	 public long incrementColumn(String rowkey,String columnFamily,String columnQualify,long incrValue) {
		 long l = 0;
		 try {
			 l=table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualify), incrValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return l;
	 }
	 
	 /** 对某个行里的多个cell自增*/
	 public List<Long> incrementRow(String rowkey){
		 Increment increment=new Increment(Bytes.toBytes(rowkey));  
	        increment.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("qualify1"), 0);  
	        increment.addColumn(Bytes.toBytes("family2"), Bytes.toBytes("qualify2"), -1);  
	        increment.addColumn(Bytes.toBytes("family3"), Bytes.toBytes("qualify3"), 2);  
	        
	        Result result = null;
			try {
				result = table.increment(increment);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			List<Long> ls = new ArrayList<Long>();
	        for(KeyValue kv:result.raw()){  
	            System.out.println(kv+"-----" + Bytes.toLong(kv.getValue()));
	            ls.add(Bytes.toLong(kv.getValue()));
	        }
	        return ls;
	 }
	 
	/** 测试事务*//*
	@SuppressWarnings("deprecation")
	public void transcation(String rowkey){
		 try {
			RowLock lock = table.lockRow(Bytes.toBytes(rowkey));
			table.put(new Put(Bytes.toBytes(rowkey)));
			table.put(new Put(Bytes.toBytes(rowkey)));
			table.put(new Put(Bytes.toBytes(rowkey)));
			table.unlockRow(lock);
		 } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }*/
	 
}
