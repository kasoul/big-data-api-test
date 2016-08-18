package com.superh.hz.bigdata.api.network.thrift;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class HelloServiceServer {
	/**
	 * 启动 Thrift 服务器
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// 设置服务端口为 8888
			TServerTransport serverTransport = new TServerSocket(8888);
			// 设置协议工厂为 TBinaryProtocol.Factory
			Factory proFactory = new TBinaryProtocol.Factory();
			// 关联处理器与 Hello 服务的实现
			TProcessor processor = new Hello.Processor(new HelloServiceImpl());
			
			//TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
			
			TThreadPoolServer.Args ttpsargs = new TThreadPoolServer.Args(serverTransport);
			ttpsargs.processor(processor);
			ttpsargs.protocolFactory(proFactory);
			
			TServer server = new TThreadPoolServer(ttpsargs);


			System.out.println("Start server on port 8888...");
			server.serve();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}
}