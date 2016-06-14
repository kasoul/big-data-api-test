package test.superh.hz.bigdata.api.hbase.table;

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
 *  2015-4-3
 */
public class TableCreator {
	private static HBaseAdmin admin = null;
	private static String tableName = "test_table_1";
	
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
		if(admin.tableExists(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		System.out.println(admin.tableExists(tableName));
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor coldef = new HColumnDescriptor("cf_1");
		coldef.setMaxVersions(6);
		coldef.setCompressionType(Compression.Algorithm.SNAPPY);
		desc.addFamily(coldef);
		admin.createTable(desc);
		System.out.println(admin.tableExists(tableName));
		System.out.println(admin.isTableAvailable(tableName));
	}

}
