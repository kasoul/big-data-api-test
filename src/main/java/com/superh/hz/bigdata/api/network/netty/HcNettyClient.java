package com.superh.hz.bigdata.api.network.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;


/** 
 * Netty 客户端代码 
 *  
 */  

public class HcNettyClient {
    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 8888;
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap client = new Bootstrap(); // (1)
            client.group(workerGroup); // (2)
            client.channel(NioSocketChannel.class); // (3)
            client.option(ChannelOption.TCP_NODELAY, true); // (4)
            client.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new HcNettyClientHandler());
                }
            });

            // Start the client.
            ChannelFuture f = client.connect(host, port).sync(); // (5)

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}