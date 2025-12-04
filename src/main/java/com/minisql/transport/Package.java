package com.minisql.transport;

public class Package {
    byte[] data;
    Exception exc;

    public Package(byte[] data, Exception exc) {
        this.data = data;
        this.exc = exc;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getExc() {
        return exc;
    }
}
