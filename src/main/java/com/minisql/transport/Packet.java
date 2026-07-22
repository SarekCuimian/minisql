package com.minisql.transport;

public class Packet {
    private final byte[] data;
    private final Exception error;

    public Packet(byte[] data, Exception error) {
        this.data = data;
        this.error = error;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getError() {
        return error;
    }
}
