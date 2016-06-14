package com.superh.hz.bigdata.api.network.rpc;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
 
import java.io.IOException;
 

public class RPCServerStart {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Server server = new RPC.Builder(conf).setProtocol(MyProtocol.class)
                .setInstance(new MyProtocolImpl())
                .setBindAddress("localhost").setPort(9000).setNumHandlers(3).build();
        server.start();
 
    }
}