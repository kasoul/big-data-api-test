package com.superh.hz.bigdata.api.network.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;


public class MyProtocolImpl implements MyProtocol {

    @Override
    public String echo(String value) throws IOException {
        return value;
    }

    @Override
    public int add(int v1, int v2) throws IOException {
        return v1 + v2;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature(versionID,null);
    }
}