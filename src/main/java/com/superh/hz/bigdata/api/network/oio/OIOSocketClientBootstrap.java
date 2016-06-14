package com.superh.hz.bigdata.api.network.oio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 发送一个socket到服务器，客户端立刻关闭
 */
public class OIOSocketClientBootstrap {
	private static final Logger logger = LoggerFactory.getLogger(OIOSocketClientBootstrap.class);
	private static final String serverIp = "127.0.0.1";
	private static final int serverPort = 8899;
	
	public static void main(String[] args) {
		OIOSocketClientBootstrap clientBootstrap = new OIOSocketClientBootstrap();
		clientBootstrap.run();
	}
	
	public void run() {
		Socket socket = null;  
	    try {  
	    	//创建一个流套接字并将其连接到指定主机上的指定端口号  
	        socket = new Socket(serverIp, serverPort);
	        InputStream istream = socket.getInputStream(); 
	        OutputStream ostream = socket.getOutputStream(); 
	        String clientData = "test client data.[end]";
	        ostream.write(clientData.getBytes());
	        ostream.flush();
	        byte[] b = new byte[256];
	        StringBuffer sb = new StringBuffer();
			while (istream.read(b) !=-1) {	
				sb.append(new String(b)); 
			}
	        ostream.close();
			istream.close();
	        logger.info("server return is : " + sb.toString());    
	     } catch (Exception e) {  
	    	logger.error("client error:" + e.getMessage(),e);   
	     } finally { 
	    	 try {  
	    		if (socket != null)  
	    			socket.close();  
	         } catch (IOException e) {
	            	socket = null;   
	                logger.error("client close error:" + e.getMessage(),e);   
	    	 } 
	     } 
	}
	
}
