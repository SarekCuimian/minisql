package com.minisql.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

public class PacketChannelTest {
    @Test
    public void preservesFailureStatus() throws Exception {
        PacketCodec codec = new PacketCodec();
        Packet decoded = codec.decode(codec.encode(Packet.error(new RuntimeException("failed"))));

        assertFalse(decoded.isSuccess());
        assertEquals("failed", decoded.getError().getMessage());
    }

    @Test
    public void exchangesPackets() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try(ServerSocket serverSocket = new ServerSocket(0)) {
            Future<?> server = executor.submit(() -> {
                try(Socket socket = serverSocket.accept();
                    PacketChannel channel = new PacketChannel(new Transporter(socket), new PacketCodec())) {
                    Packet first = channel.receive();
                    assertTrue(first.isSuccess());
                    assertArrayEquals("packet-1".getBytes(StandardCharsets.UTF_8), first.getPayload());
                    Packet second = channel.receive();
                    assertTrue(second.isSuccess());
                    assertArrayEquals("packet-2".getBytes(StandardCharsets.UTF_8), second.getPayload());
                    channel.send(Packet.data("packet-3".getBytes(StandardCharsets.UTF_8)));
                }
                return null;
            });

            try(Socket socket = new Socket("127.0.0.1", serverSocket.getLocalPort());
                PacketChannel channel = new PacketChannel(new Transporter(socket), new PacketCodec())) {
                channel.send(Packet.data("packet-1".getBytes(StandardCharsets.UTF_8)));
                channel.send(Packet.data("packet-2".getBytes(StandardCharsets.UTF_8)));
                Packet response = channel.receive();
                assertTrue(response.isSuccess());
                assertArrayEquals("packet-3".getBytes(StandardCharsets.UTF_8), response.getPayload());
            }
            server.get();
        } finally {
            executor.shutdownNow();
        }
    }
}
