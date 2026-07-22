package com.minisql.transport;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.common.primitives.Bytes;
import com.minisql.common.Error;

public final class PacketCodec {

    private static final byte SUCCESS_STATUS = 0;
    private static final byte ERROR_STATUS = 1;

    public byte[] encode(Packet packet) {
        if(packet.getError() != null) {
            String message = "Internal server error!";
            if(packet.getError().getMessage() != null) {
                message = packet.getError().getMessage();
            }
            return Bytes.concat(new byte[]{ERROR_STATUS}, message.getBytes(StandardCharsets.UTF_8));
        }
        return Bytes.concat(new byte[]{SUCCESS_STATUS}, packet.getData());
    }

    public Packet decode(byte[] data) throws Exception {
        if(data == null || data.length < 1) {
            throw Error.MalformedPacketException;
        }
        if(data[0] == SUCCESS_STATUS) {
            return new Packet(Arrays.copyOfRange(data, 1, data.length), null);
        }
        if(data[0] == ERROR_STATUS) {
            String message = new String(data, 1, data.length - 1, StandardCharsets.UTF_8);
            return new Packet(null, new RuntimeException(message));
        }
        throw Error.MalformedPacketException;
    }
}
