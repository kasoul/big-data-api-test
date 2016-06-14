package cmcc.zyhy.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.superh.hz.bigdata.api.hbase.dao.HBaseClient;


public class TestHBaseClient {
	private static HBaseClient hbaseClient = HBaseClient.getClient();

	public static void main(String args[]) throws IOException{
		test_isTableAvailable();
	}
	
	/**
	 *测试表的可用性 
	 * 
	 */
	private static void test_isTableAvailable() throws IOException{
		System.out.println(hbaseClient.existsTable("test"));
	}
	
	/**
	 *获取所有的表名 
	 * 
	 */
	private static void test_listTables() throws IOException{
		String[] tables = hbaseClient.listTables();
		for(String tableName : tables){
			System.out.println(tableName);
		}
	}
}
