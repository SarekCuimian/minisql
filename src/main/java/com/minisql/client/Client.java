package com.minisql.client;

import com.minisql.common.ExecResult;
import com.minisql.common.ExecResultCodec;
import com.minisql.transport.Package;
import com.minisql.transport.Packager;

public class Client {
    private final RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public ExecResult execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getExc() != null) {
            throw resPkg.getExc();
        }
        return ExecResultCodec.decode(resPkg.getData());
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
