package com.spuerh.hz.bigdata.mr.util.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * 工具类，map-reduce作业启动的时候获取配置信息
 */
public class ConfUtil 
{
	private static Logger log = LoggerFactory.getLogger(ConfUtil.class);
	private static String CONF_BASE_PATH = "../conf/";
	
	public static Configuration getConf(Configuration conf)
	{
		conf.addResource(new Path(CONF_BASE_PATH + "hdfs-site.xml"));	
		return conf;
	}
	
	public static Configuration getHBaseConf(Configuration conf)
	{
		conf.addResource(new Path(CONF_BASE_PATH + "hbase-site.xml"));
		
		log.info("hbase.security.authentication [{}]", conf.get("hbase.security.authentication", ""));
		log.info("hbase.security.authorization [{}]", conf.get("hbase.security.authorization", ""));
		
		return getConf(conf);
	}
	
	/** 
	 * 读取配置文件：
	 * 1.conf目录下的hdfs-site.xml
	 * 2.conf目录下的hbase-site.xml
	 * 3.conf目录下的mapred-site.xml
	 * 4.conf目录下的lbs-mrjob-busType.xml,busType为具体分析业务
	 * 
	 * @param
	 * 		busType：业务类型
	 */
	public static Configuration getBussinesConf(String busType)
	{
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(ConfUtil.CONF_BASE_PATH + "hdfs-site.xml"));
		conf.addResource(new Path(ConfUtil.CONF_BASE_PATH + "hbase-site.xml"));
		conf.addResource(new Path(ConfUtil.CONF_BASE_PATH + "mapred-site.xml"));
		conf.addResource(new Path(ConfUtil.CONF_BASE_PATH + "lbs-mrjob-"+ busType +".xml"));
		
		return conf;
	}

	public static void main(String args[]){
		Configuration conf = getBussinesConf("UserWorkPlaceAnalysis");
		System.out.println(conf.get("UserWorkPlaceAnalysis.calculate.date")==null);
		System.out.println(conf.getInt("UserWorkPlaceAnalysis.calculate.days", 60));
	}
}
