package cmcc.zyhy.hbase.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;

/** 
 *
 * @Project: hbase-accessor
 * @File: TableCreator.java 
 * @Date: 2015年4月3日 
 * @Author: 黄超（huangchaohz）
 * @Copyright: 版权所有 (C) 2015 中国移动 杭州研发中心. 
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
 */

/**
 *  @Describe:生成数据表
 */
public class TableCreator {
	private static HBaseAdmin admin = null;
	
	public static void main(String args[]){
		initAdmin();
		try {
			create_user_month_bill_table();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void initAdmin(){
		try {
			admin = new HBaseAdmin(HBaseConfiguration.create(new Configuration()));
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
	
	private static void create_user_month_bill_table() throws IOException{
		if(admin.tableExists("user_month_bill_table")){
			admin.disableTable("user_month_bill_table");
			admin.deleteTable("user_month_bill_table");
		}
		System.out.println(admin.tableExists("user_month_bill_table"));
		HTableDescriptor desc = new HTableDescriptor("user_month_bill_table");
		HColumnDescriptor coldef = new HColumnDescriptor("bill");
		coldef.setMaxVersions(6);
		coldef.setCompressionType(Compression.Algorithm.SNAPPY);
		desc.addFamily(coldef);
		admin.createTable(desc);
		System.out.println(admin.tableExists("user_month_bill_table"));
		System.out.println(admin.isTableAvailable("user_month_bill_table"));
	}

}
