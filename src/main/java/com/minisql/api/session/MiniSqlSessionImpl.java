package com.minisql.api.session;

import com.minisql.common.ExecutionResult;
import com.minisql.common.ExecutionResultCodec;
import com.minisql.transport.PacketChannel;
import com.minisql.transport.PacketCodec;
import com.minisql.transport.Transporter;
import com.minisql.transport.Packet;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class MiniSqlSessionImpl implements MiniSqlSession {

    private final PacketChannel packetChannel;
    private final Socket socket;
    private final Object ioLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MiniSqlSessionImpl(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        Transporter transporter = new Transporter(socket);
        PacketCodec codec = new PacketCodec();
        this.packetChannel = new PacketChannel(transporter, codec);
    }

    @Override
    public ExecutionResult execute(String sql) throws Exception {
        ensureOpen();
        String statement = Objects.requireNonNull(sql, "sql must not be null").trim();
        if(statement.isEmpty()) {
            throw new IllegalArgumentException("SQL must not be empty");
        }
        // 保障单个 Session 的 Socket 线程安全
        synchronized (ioLock) {
            Packet requestPacket = new Packet(statement.getBytes(StandardCharsets.UTF_8), null);
            packetChannel.send(requestPacket);
            Packet responsePacket = packetChannel.receive();
            if (responsePacket.getError() != null) {
                throw responsePacket.getError();
            }
            // 传输解包（去掉状态位）后，用结果序列化层恢复 ExecutionResult
            return ExecutionResultCodec.decode(responsePacket.getData());
        }
    }

    @Override
    public void close() {
        if(!closed.compareAndSet(false, true)) {
            return;
        }
        Exception closeErr = null;
        try {
            packetChannel.close();
        } catch (Exception e) {
            closeErr = e;
        }
        try {
            socket.close();
        } catch (IOException ioe) {
            if(closeErr == null) {
                closeErr = ioe;
            } else {
                closeErr.addSuppressed(ioe);
            }
        }
        if(closeErr != null) {
            throw new RuntimeException("Failed to close MiniSqlEngineImpl", closeErr);
        }
    }

    private void ensureOpen() {
        if(closed.get()) {
            throw new IllegalStateException("MiniSqlEngineImpl already closed");
        }
    }
}
