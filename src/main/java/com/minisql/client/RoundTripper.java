package com.minisql.client;

import com.minisql.transport.Packet;
import com.minisql.transport.PacketChannel;

public class RoundTripper {
    private final PacketChannel packetChannel;

    public RoundTripper(PacketChannel packetChannel) {
        this.packetChannel = packetChannel;
    }

    public Packet roundTrip(Packet packet) throws Exception {
        packetChannel.send(packet);
        return packetChannel.receive();
    }

    public void close() throws Exception {
        packetChannel.close();
    }
}
