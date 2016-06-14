package com.superh.hz.bigdata.api.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Configeration Builder
 */
public class ConfigerationBuilder {
	
	private static final Logger log = LoggerFactory.getLogger(ConfigerationBuilder.class);
	
	public static void main(String args[]){
		System.out.println(getJarPath());
		Configuration config = getHBaseConfigurationInFileSystem();
		System.out.println(config.get("fs.defaultFS"));
		System.out.println(config.get("hbase.zookeeper.quorum"));
	}
	
	public static Configuration getConfigurationInClassPath(){
		return new Configuration();
	}
	
	/**
	 * 加载hdfs客户端的配置文件
	 * core-site.xml
	 * hdfs-site.xml
	 */
	public static Configuration getConfigurationInFileSystem(){
		Configuration config = new Configuration();
		String path = getJarPath();
		config.addResource(new Path(path + "conf/core-site.xml"));
		config.addResource(new Path(path + "conf/hdfs-site.xml"));
		return config;
	}
	
	/**
	 * 加载hbase客户端的配置文件
	 * core-site.xml
	 * hbase-site.xml
	 */
	public static Configuration getHBaseConfigurationInFileSystem(){
		Configuration config = HBaseConfiguration.create();
		String path = getJarPath();
		config.addResource(new Path(path + "conf/core-site.xml"));
		config.addResource(new Path(path + "conf/hdfs-site.xml"));
		config.addResource(new Path(path + "conf/hbase-site.xml"));
		return config;
	}
	
	
	/**
	 * 获取classpath路径
	 * 如果打成jar包，获取的就是jar的同目录
	 * 
	 */
	public static String getJarPath(){
		String path = ConfigerationBuilder.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		try{  
	        path = java.net.URLDecoder.decode(path, "UTF-8");//转换处理中文及空格 
	    }catch (java.io.UnsupportedEncodingException e){
	    	log.error("get configuration path error",e);
	    }
		if (path.endsWith(".jar")){
			path = path.substring(0, path.lastIndexOf("/") + 1);
		}
		return path;
	}
}
