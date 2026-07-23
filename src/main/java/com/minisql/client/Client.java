package com.minisql.client;

import com.minisql.result.ExecutionResult;
import com.minisql.result.ExecutionResultCodec;
import com.minisql.transport.Packet;
import com.minisql.transport.PacketChannel;

public class Client {
    private final RoundTripper rt;

    public Client(PacketChannel packetChannel) {
        this.rt = new RoundTripper(packetChannel);
    }

    public ExecutionResult execute(byte[] stat) throws Exception {
        Packet requestPacket = Packet.data(stat);
        Packet responsePacket = rt.roundTrip(requestPacket);
        if(!responsePacket.isSuccess()) {
            throw responsePacket.getError();
        }
        // 传输层收到的 payload，再经结果序列化层解码为 ExecutionResult
        return ExecutionResultCodec.decode(responsePacket.getPayload());
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception ignored) {
        }
    }

}
