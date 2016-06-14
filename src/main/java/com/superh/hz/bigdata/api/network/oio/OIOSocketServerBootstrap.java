package com.superh.hz.bigdata.api.network.oio;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  发送一个socket到服务器，客户端立刻关闭
 */
public class OIOSocketServerBootstrap {
	private static final Logger logger = LoggerFactory.getLogger(OIOSocketServerBootstrap.class);
	private static final int serverPort = 8899;
	
	public static void main(String[] args) {
		OIOSocketServerBootstrap bootstrap = new OIOSocketServerBootstrap();
		bootstrap.run();
	}
	
	public void run() {
		try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
			logger.info("ServerSocket have been started and Listening on port {}",serverSocket.getLocalPort());
			while (true) {
				Socket socket = serverSocket.accept();
				//对于不同的socket请求包，正规做法是用多线程处理才是正确做法，此处为简易做法
				String clientInfo = null;
				if (socket != null){
					clientInfo = socket.getInetAddress().toString();
					logger.info("client address is {}.",clientInfo);
				}	
				try {
					InputStream istream = socket.getInputStream();
					OutputStream ostream = socket.getOutputStream();
					byte[] b = new byte[256];
					StringBuffer sb = new StringBuffer(); 
					if (istream.read(b) !=-1) {
						sb.append(new String(b)); 
						String trimString = sb.toString().trim();
						logger.info("client data received is [{}].", trimString); 
						ostream.write(trimString.getBytes()); 
						ostream.flush();
					}else{
						logger.error("no data from client input stream.");
						ostream.write("null".getBytes()); 
						ostream.flush();
					}
					ostream.close();
					istream.close();
				}
				catch (IOException err) {
					logger.error("server run error. ",err);
				} finally {
					logger.info("[{}] disconnect.", clientInfo);
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
}
