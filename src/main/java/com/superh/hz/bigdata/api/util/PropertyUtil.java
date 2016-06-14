package com.superh.hz.bigdata.api.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 *
 *
 * @Project: big-apple-api-test
 * @File: PropertyUtil.java 
 * @Date: 2014年11月28日 
 * @Author: 李晓平
 */

public class PropertyUtil {
	
	private static final Logger log = LoggerFactory.getLogger(PropertyUtil.class);
	private static Properties p =  new Properties();
	
	static{
		init();
	}
	
	
	public static void init(){
		  
		
		try {
			String path = getJarPath();
			p.load(new FileInputStream(path + "conf/redis.properties"));
			
		} catch (IOException e) {
			log.error("配置文件加载失败", e);
		}
		
		
	}
	
	
	public static String getUnicodeValue(String key){
		
		Object unicode = p.get(key);
		
		if(unicode == null ||"".equals(unicode)) return "utf-8";
		
		return unicode.toString();
	}
	
	
	public static String getUrlRegex(){
		
		return p.get("regex").toString();
	}
	
	public static String getJarPath(){
		String path = PropertyUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		try{  
	            path = java.net.URLDecoder.decode(path, "UTF-8");//转换处理中文及空格  
	    }catch (java.io.UnsupportedEncodingException e){
	           
	    }
		if (path.endsWith(".jar")){
			path = path.substring(0, path.lastIndexOf("/") + 1);
		}
		return path;
	}
	

}
