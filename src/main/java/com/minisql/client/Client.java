package com.minisql.client;

import com.minisql.common.ExecResult;
import com.minisql.common.ExecResultEncoder;
import com.minisql.transport.Package;
import com.minisql.transport.Packager;

public class Client {
    private final RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public ExecResult execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package respkg = rt.roundTrip(pkg);
        if(respkg.getExc() != null) {
            throw respkg.getExc();
        }
        // 传输层收到的 payload，再经结果序列化层解码为 ExecResult
        return ExecResultEncoder.decode(respkg.getData());
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
