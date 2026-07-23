package com.minisql.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import org.apache.commons.codec.binary.Hex;

public class Transporter {
    private final Socket socket;
    private final BufferedReader reader;
    private final BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public void send(byte[] bytes) throws Exception {
        String encodedLine = Hex.encodeHexString(bytes, true);
        writer.write(encodedLine);
        writer.newLine();
        writer.flush();
    }

    public byte[] receive() throws Exception {
        String encodedLine = reader.readLine();
        if(encodedLine == null) {
            close();
            throw new EOFException("peer closed the transport connection");
        }
        return Hex.decodeHex(encodedLine);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

}
