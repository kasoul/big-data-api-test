package com.superh.hz.bigdata.api.network.rpc;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
 
import java.io.IOException;
import java.net.InetSocketAddress;
 

public class RPCClientTest {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        InetSocketAddress address = new InetSocketAddress("localhost",9000);
        MyProtocol proxy = (MyProtocol) RPC.getProxy(MyProtocol.class,MyProtocol.versionID,
               address ,conf);
 
        System.out.println(proxy.add(5, 6));
    }
}