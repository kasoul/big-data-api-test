package cmcc.zyhy.hbase.dao;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
/*import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;*/
import org.apache.hadoop.hbase.util.Bytes;

import com.superh.hz.bigdata.api.hbase.dao.HBaseDataManager;

/** 
 *
 *
 * @Project: hbase-accessor
 * @File: TestHBaseDML.java 
 * @Date: 2014年11月28日 
 * @Author: 黄超（huangchaohz）
 * @Copyright: 版权所有 (C) 2014 中国移动 杭州研发中心. 
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
 */

/**
 *  @Describe:
 */
public class TestHBaseDML {
	private static HBaseDataManager hbaseDataManager = HBaseDataManager.getHBaseDataManager("test_table");
	private static HBaseDataManager metaDataManager = HBaseDataManager.getHBaseDataManager("hbase:meta");

	
	public static void main(String args[]) throws IOException{
		test_queryData();
	}

	/**
	 *查询meta，获取所有的region信息
	 * 
	 */
	private static void test_checkregion() throws IOException{
		//WritableByteArrayComparable a =  new BinaryComparator(Bytes.toBytes("belong"));
		Filter filter = new QualifierFilter(CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("regioninfo")));
		Set<String> rows = metaDataManager.queryAll();
		for(String row:rows){
			if(row.contains("user_position_table")){
				System.out.println(row);
			}
		}
		
	}
	
	/**
	 *插入数据
	 * 
	 */
	private static void test_insertData() throws IOException{
		/*hbaseDataManager.insertData("hc", "user", "age", Bytes.toBytes(26));
		hbaseDataManager.queryAll();*/
		/*String value1 = "1=18867102770=hh=2=S:10:20150315020202|R:10:20150316020202|R:10:20150316080202;";
		String value2 = "20150316:120.168,31.3546|120.168,30.3546|120.168,30.3046;20150317:120.168,30.3546|120.168,31.3546|120.168,30.3146;";*/
		
		String value1 = "1=18867102770=hh=2=S:10:20150315020202|R:10:20150316020202|R:10:20150316080202;";
		String value2 = "20150316:120.168,31.3546|120.168,30.3546|120.168,30.3046;20150317:120.168,30.3546|120.168,31.3546|120.168,30.3146;";
		hbaseDataManager.insertData("18867102870", "dynamicInfo", "recentContactors", Bytes.toBytes(value1));
		hbaseDataManager.insertData("18867102870", "dynamicInfo", "recentActiveAddrNightCoors", Bytes.toBytes(value2));
		

	}
	
	/**
	 *查询该表所有的数据 
	 * 
	 */
	private static void test_queryData() throws IOException{
		hbaseDataManager.queryByRowkeyAndQualify("TOM", "phone", "");
	}
	
	/**
	 *查询某个列的值
	 * 
	 */
	private static void test_getData() throws IOException{
		//hbaseDataManager.queryByRowkeyAndQualify("18810000000-20150331233000", "position", "duration");
		
	}
	
	/**
	 *删除某行
	 * 
	 */
	private static void test_deleteData() throws IOException{
		hbaseDataManager.deleteData("1");
		hbaseDataManager.queryAll();
	}
	
	/**
	 *测试查询过滤器
	 * 
	 */
	private static void test_filter() throws IOException{
		hbaseDataManager.queryByFilter( "basicInfo", "name","26");
	}
	
	/**
	 *查询计数器
	 * 
	 */
	private static void test_counter() throws IOException{
		System.out.println(hbaseDataManager.incrementColumn("2", "user", "name", 3));
		hbaseDataManager.queryByRowkeyAndQualify("1", "user", "counter");
		hbaseDataManager.queryAll();
	}
}
