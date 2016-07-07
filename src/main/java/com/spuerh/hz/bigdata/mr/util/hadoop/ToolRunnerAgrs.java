package com.spuerh.hz.bigdata.mr.util.hadoop;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  @Describe:ToolRunner的参数设置
 */
public class ToolRunnerAgrs {

	private static Logger log = LoggerFactory.getLogger(ToolRunnerAgrs.class);
	private static String lib_path = "../lib";
	private static String[] agrs = null;
	
	static 
	{
		agrs = new String[2];
		File catalogPath = new File(lib_path);
		if(!catalogPath.isDirectory()){
			log.info("The lib path {} is not a directory,please check.",catalogPath.getAbsolutePath());
			System.exit(1);
		}else{
			agrs[0] = "-libjars";
			agrs[1] = "";
			File[] jars = catalogPath.listFiles();
			log.info("Third part jars inputed into classpath,number is {}.",jars.length);
			for(int i=0;i<jars.length;i++){
				agrs[1] += jars[i].getAbsolutePath();
				log.info("Third part jar [{}] inputed into classpath.",jars[i].getAbsolutePath());
				if(i!=(jars.length-1)){
					agrs[1] +=",";
				}
			}
			log.info("Third part jars is [{}] inputed into classpath.",agrs[1]);
		}
	}
	
	public static String[] getAgrs(){
		return agrs;
	}
	
	public static void setAgrs(String[] agrs){
		ToolRunnerAgrs.agrs = agrs ;
	}
	
	public static void addAgrs(String[] agrs){
		String[] newAgrs = new String[ToolRunnerAgrs.agrs.length + agrs.length];
		for(int i=0;i<ToolRunnerAgrs.agrs.length;i++){
			newAgrs[i] = ToolRunnerAgrs.agrs[i];
		}
		for(int i=0;i<agrs.length;i++){
			newAgrs[i+ToolRunnerAgrs.agrs.length] = agrs[i];
		}
		ToolRunnerAgrs.agrs = newAgrs ;
	}
	

}
