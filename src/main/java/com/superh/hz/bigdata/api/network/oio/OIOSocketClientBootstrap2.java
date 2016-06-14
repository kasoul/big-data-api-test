package com.superh.hz.bigdata.api.network.oio;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * @Project: network-test
 * @File: OIOSocketClientBootstrap2.java 
 * @Date: 2015年10月19日 
 * @Author: 黄超（huangchaohz）
 * @Copyright: 版权所有 (C) 2015 中国移动 杭州研发中心. 
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
 */

/**
 *  @Describe:在一个Socket里持续发送数据到ServerSocket,知道接收到服务器发送的OK
 */
public class OIOSocketClientBootstrap2 {
	private static final Logger logger = LoggerFactory.getLogger(OIOSocketClientBootstrap2.class);
	private static final String serverIp = "127.0.0.1";
	private static final int serverPort = 8899;
	
	public static void main(String[] args) {
		OIOSocketClientBootstrap2 clientBootstrap = new OIOSocketClientBootstrap2();
		clientBootstrap.run();
	}

	/** 
	  * @description 
	  * @author 黄超(huangchaohz)
	  * @date 2015年10月19日
	  * @param 
	  * @return 
	  */
	private void run() {	
		while (true) {    
	    	Socket socket = null;  
	        try {  
	        	//创建一个流套接字并将其连接到指定主机上的指定端口号  
	            socket = new Socket(serverIp, serverPort);    
	                    
	            //读取服务器端数据    
	            DataInputStream input = new DataInputStream(socket.getInputStream());  
	            //向服务器端发送数据    
	            DataOutputStream out = new DataOutputStream(socket.getOutputStream()); 
	            logger.info("客户端请输入: \t");
	            String str = new BufferedReader(new InputStreamReader(System.in)).readLine();
	            out.writeUTF(str); 
	            
	            String ret = input.readUTF(); 
	            logger.info("服务器端返回过来的是: " + ret); 
	            // 如接收到 "OK" 则断开连接  
	            if ("OK".equals(ret)) { 
	            	logger.info("客户端将关闭连接");
	                Thread.sleep(500); 
	                break;
	            }    
	                  
	            out.close();  
	            input.close();  
	         } catch (Exception e) {  
	            logger.error("客户端异常:" + e.getMessage(),e);   
	         } finally {  
	            if (socket != null) {  
	            	try {  
	                	socket.close();  
	                } catch (IOException e) {  
	                	socket = null;   
	                	logger.error("客户端 finally 异常:" + e.getMessage(),e);   
	                }
	            }
	         }
	    }
	}
}
