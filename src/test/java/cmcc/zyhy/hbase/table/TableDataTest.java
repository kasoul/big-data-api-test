package cmcc.zyhy.hbase.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 *
 * @Project: hbase-accessor
 * @File: TableDataTest.java 
 * @Date: 2015年4月3日 
 * @Author: 黄超（huangchaohz）
 * @Copyright: 版权所有 (C) 2015 中国移动 杭州研发中心. 
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
 */

/**
 *  @Describe:测试数据表
 */
public class TableDataTest {
	private static HTableInterface table = null;
	
	public static void main(String args[]){
		initHTable();
		try {
			test_user_month_bill_table();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void initHTable(){
		try {
			table = new HTable(HBaseConfiguration.create(new Configuration()),"user_month_bill_table");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void test_user_month_bill_table() throws IOException{
		  try { 
	            Get get = new Get(Bytes.toBytes("18766688888"));// 根据rowkey查询
	            //System.out.println(get.getMaxVersions());
	            get.setMaxVersions(8);
	            System.out.println(get.getMaxVersions());
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

}
