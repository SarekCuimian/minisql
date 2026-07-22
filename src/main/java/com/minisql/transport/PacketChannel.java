package com.minisql.transport;

/**
 * 面向 {@link Packet} 的同步双向通道，负责协议编解码和底层字节传输。
 */
public final class PacketChannel implements AutoCloseable {
    private final Transporter transporter;
    private final PacketCodec codec;

    public PacketChannel(Transporter transporter, PacketCodec codec) {
        this.transporter = transporter;
        this.codec = codec;
    }

    public void send(Packet packet) throws Exception {
        transporter.send(codec.encode(packet));
    }

    public Packet receive() throws Exception {
        return codec.decode(transporter.receive());
    }

    @Override
    public void close() throws Exception {
        transporter.close();
    }
}
