package com.spuerh.hz.bigdata.mr.util.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  @Describe:加密工具
 */
public class EncryptUtil {

	private static final Logger logger = LoggerFactory.getLogger(EncryptUtil.class);
	
	/**
	 * MD5加密函数
	 * @param source源字符串 type加密类型(MD5,SHA)
	 * @return
	 */
	public static String encrypt(String source, String type) {
		StringBuilder sb = new StringBuilder();
	    MessageDigest md5;
	    try {
	      md5 = MessageDigest.getInstance(type);
	      md5.update(source.getBytes());
	      for (byte b : md5.digest()) {
	        sb.append(String.format("%02X", b)); // 10进制转16进制，X 表示以十六进制形式输出，02 表示不足两位前面补0输出
	      }
	      return sb.toString();
	    } catch (NoSuchAlgorithmException e) {
	      logger.error(e.getMessage());
	      return null;
	    }
	}

}
