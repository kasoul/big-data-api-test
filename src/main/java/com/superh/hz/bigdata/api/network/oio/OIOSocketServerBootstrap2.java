package com.superh.hz.bigdata.api.network.oio;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  在一个Socket里持续发送数据到ServerSocket,直到接收到服务器发送的OK
 *  2015-10-19
 */
public class OIOSocketServerBootstrap2 {
	private static final Logger logger = LoggerFactory.getLogger(OIOSocketServerBootstrap2.class);
	private static final int serverPort = 8899;
	
	public static void main(String[] args) {
		OIOSocketServerBootstrap2 bootstrap = new OIOSocketServerBootstrap2();
		bootstrap.run();
	}
	
	public void run() {    
		try {    
	    	ServerSocket serverSocket = new ServerSocket(serverPort);
	    	logger.info("ServerSocket have been started and Listening on port {}",serverSocket.getLocalPort());
	    	while (true) {    
	                // 一旦有堵塞, 则表示服务器与客户端获得了连接    
	                Socket client = serverSocket.accept();    
	                // 处理这次连接    
	                handler(client);    
	    	}    
		} catch (Exception e) {    
			logger.error("服务器异常: " + e.getMessage(),e);    
		} 
	} 


	private void handler(Socket socket) {
		 try {    
             // 读取客户端数据    
             DataInputStream input = new DataInputStream(socket.getInputStream());  
             String clientInputStr = input.readUTF();//这里要注意和客户端输出流的写方法对应,否则会抛 EOFException  
             // 处理客户端数据    
             logger.info("客户端发过来的内容:" + clientInputStr);    
 
             // 向客户端回复信息    
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());    
             logger.info("服务器端请输入:\t");    
             // 发送键盘输入的一行    
             String s = new BufferedReader(new InputStreamReader(System.in)).readLine();    
             out.writeUTF(s);
             
             out.close();    
             input.close();    
         } catch (Exception e) {    
        	 logger.error("服务器 run 异常: " + e.getMessage(),e);    
         } finally {    
             if (socket != null) {    
                 try { 
                     socket.close();    
                 } catch (Exception e) {    
                     socket = null;    
                     logger.error("服务端 finally 异常:" + e.getMessage(),e);    
                 }    
             }    
         }   
     }    
}
