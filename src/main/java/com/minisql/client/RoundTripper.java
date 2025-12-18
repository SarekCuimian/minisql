package com.minisql.client;

import com.minisql.transport.Package;
import com.minisql.transport.Packager;

public class RoundTripper {
    private final Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
