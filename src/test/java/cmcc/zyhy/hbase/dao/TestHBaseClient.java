package cmcc.zyhy.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.superh.hz.bigdata.api.hbase.dao.HBaseClient;

/** 
 *
 *
 * @Project: hbase-accessor
 * @File: TestHBaseClient.java 
 * @Date: 2014年11月28日 
 * @Author: 黄超（huangchaohz）
 * @Copyright: 版权所有 (C) 2014 中国移动 杭州研发中心. 
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
 */

/**
 *  @Describe:
 */
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
