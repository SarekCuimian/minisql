package com.minisql.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import com.minisql.transport.PacketChannel;
import com.minisql.transport.PacketCodec;
import com.minisql.transport.Transporter;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        PacketCodec codec = new PacketCodec();
        Transporter transporter = new Transporter(socket);
        PacketChannel packetChannel = new PacketChannel(transporter, codec);

        Client client = new Client(packetChannel);
        Shell shell = new Shell(client);
        shell.run();
    }
}
