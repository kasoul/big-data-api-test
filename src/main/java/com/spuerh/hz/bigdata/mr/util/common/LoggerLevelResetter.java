package com.spuerh.hz.bigdata.mr.util.common;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/** 
 *
 *
 * @Project: migunet-mapreduce
 * @File: LoggerConfiguration.java 
 * @Date: 2015年6月15日 
 * @Author: ChengShi
 * @Copyright: 版权所有 (C) 2015 中国移动 杭州研发中心. 
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
 */
public class LoggerLevelResetter
{	
	private static Logger log = Logger.getLogger(LoggerLevelResetter.class);
	public static void setLoggerLevel(String level)
	{
		switch (level)
		{
			case "debug" :
				Logger.getRootLogger().setLevel(Level.DEBUG);
				break;
			case "info" :
				Logger.getRootLogger().setLevel(Level.INFO);
				break;
			case "warn" :
				Logger.getRootLogger().setLevel(Level.WARN);
				break;
			case "error" :
				Logger.getRootLogger().setLevel(Level.ERROR);
				break;
			default :
				log.info("the giving log level not match debug or info or warn or error");
				break;
		}
	}
}
