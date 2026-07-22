package com.minisql.client;

import com.minisql.common.ExecutionResult;
import com.minisql.common.ExecutionResultCodec;
import com.minisql.transport.Packet;
import com.minisql.transport.PacketChannel;

public class Client {
    private final RoundTripper rt;

    public Client(PacketChannel packetChannel) {
        this.rt = new RoundTripper(packetChannel);
    }

    public ExecutionResult execute(byte[] stat) throws Exception {
        Packet requestPacket = new Packet(stat, null);
        Packet responsePacket = rt.roundTrip(requestPacket);
        if(responsePacket.getError() != null) {
            throw responsePacket.getError();
        }
        // 传输层收到的 payload，再经结果序列化层解码为 ExecutionResult
        return ExecutionResultCodec.decode(responsePacket.getData());
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception ignored) {
        }
    }

}
