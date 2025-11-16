package com.minisql.client;

import com.minisql.backend.utils.format.ExecResult;
import com.minisql.backend.utils.format.ExecResultCodec;
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
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
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
