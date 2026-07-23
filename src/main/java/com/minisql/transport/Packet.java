package com.minisql.transport;

import java.util.Objects;

public final class Packet {
    private final boolean success;
    private final byte[] payload;
    private final Exception error;

    private Packet(boolean success, byte[] payload, Exception error) {
        this.success = success;
        this.payload = payload;
        this.error = error;
    }

    public static Packet data(byte[] payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        return new Packet(true, payload, null);
    }

    public static Packet error(Exception error) {
        Objects.requireNonNull(error, "error must not be null");
        return new Packet(false, null, error);
    }

    public boolean isSuccess() {
        return success;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Exception getError() {
        return error;
    }
}
