package com.superh.hz.bigdata.api.network.thrift;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;


/**
 * 非阻塞模式服务器
 */
public class HelloNonBlockingServer {
	
	public static final int SERVER_PORT = 8888;
 
	public void startServer() {
		try {
 
			TProcessor tprocessor =  new Hello.Processor(new HelloServiceImpl());

			TNonblockingServerSocket tnbSocketTransport = new TNonblockingServerSocket(
					SERVER_PORT);
			TNonblockingServer.Args tnbArgs = new TNonblockingServer.Args(
					tnbSocketTransport);
			tnbArgs.processor(tprocessor);
			tnbArgs.transportFactory(new TFramedTransport.Factory());
			tnbArgs.protocolFactory(new TCompactProtocol.Factory());
 
			// 使用非阻塞式IO，服务端和客户端需要指定TFramedTransport数据传输的方式
			TServer server = new TNonblockingServer(tnbArgs);
			
			System.out.println("HelloWorld TNonblockingServer start ....");
			server.serve();
 
			
		} catch (Exception e) {
			System.out.println("Server start error!!!");
			e.printStackTrace();
		}
	}
 
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HelloNonBlockingServer server = new HelloNonBlockingServer();
		server.startServer();
	}
 
}
