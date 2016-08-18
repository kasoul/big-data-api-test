package com.superh.hz.bigdata.api.network.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;


/**
 * 非阻塞模式客户端
 */
public class HelloNonBlockingClient {

	public static final String SERVER_IP = "localhost";
	public static final int SERVER_PORT = 8888;
	public static final int TIMEOUT = 30000;
 
	/**
	 *
	 * @param userName
	 */
	public void startClient() {
		TTransport transport = null;
		try {
			transport = new TFramedTransport(new TSocket(SERVER_IP,SERVER_PORT, TIMEOUT));
			// 协议要和服务端一致
			TProtocol protocol = new TCompactProtocol(transport);
			Hello.Client client = new Hello.Client(protocol);
			transport.open();
			client.helloVoid();
			System.out.println("Thrify client remote call over");
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			if (null != transport) {
				transport.close();
			}
		}
	}
 
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HelloNonBlockingClient client = new HelloNonBlockingClient();
		client.startClient();
 
	}
 
}
