package com.superh.hz.bigdata.api.network.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(NIOClient.class);
	private Selector selector;

	/**
	 * Get ServerSocket channel and initialize
	 */
	public void init(String ip, int port) throws Exception {
		SocketChannel channel = SocketChannel.open();
		channel.configureBlocking(false);
		this.selector = Selector.open();
		channel.connect(new InetSocketAddress(ip, port));
		channel.register(selector, SelectionKey.OP_CONNECT);
	}

	/**
	 * listen selector
	 */
	public void listen() throws Exception {
		while (true) {
			selector.select();
			Iterator<SelectionKey> ite = this.selector.selectedKeys().iterator();
			while (ite.hasNext()) {
				SelectionKey key = (SelectionKey) ite.next();
				ite.remove();
				if (key.isConnectable()) {
					SocketChannel channel = (SocketChannel) key.channel();
					if (channel.isConnectionPending()) {
						channel.finishConnect();
					}
					channel.configureBlocking(false);// 非阻塞

					channel.write(ByteBuffer.wrap(new String("Send test info to server").getBytes()));
					channel.register(this.selector, SelectionKey.OP_READ);
				} else if (key.isReadable()) {
					read(key);
				}
			}
		}
	}

	/**
	 * Deal client send event
	 */
	public void read(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		channel.read(buffer);
		byte[] data = buffer.array();
		String info = new String(data).trim();
		System.out.println("Client receive info : " + info);
		ByteBuffer outBuffer = ByteBuffer.wrap(info.getBytes());
		channel.write(outBuffer);
	}

	public static void main(String[] args) {
		try {
			NIOClient client = new NIOClient();
			client.init(ConfigureAPI.ServerAddress.NIO_IP,
					ConfigureAPI.ServerAddress.NIO_PORT);
			client.listen();
		} catch (Exception ex) {
			ex.printStackTrace();
			LOGGER.error("NIOClient main run has error,info is "
					+ ex.getMessage());
		}
	}
}
