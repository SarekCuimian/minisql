package com.minisql.backend.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.minisql.backend.utils.format.ExecResult;
import com.minisql.backend.utils.format.ExecResultCodec;
import com.minisql.transport.Encoder;
import com.minisql.transport.Package;
import com.minisql.transport.Packager;
import com.minisql.transport.Transporter;

public class Server {
    private int port;
    private final DatabaseProvider databaseProvider;

    public Server(int port, DatabaseProvider databaseProvider) {
        this.port = port;
        this.databaseProvider = databaseProvider;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                Socket socket = ss.accept();
                Runnable handleSocket = new HandleSocket(socket, databaseProvider);
                tpe.execute(handleSocket);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }
}

class HandleSocket implements Runnable {
    private Socket socket;
    private DatabaseProvider databaseProvider;

    public HandleSocket(Socket socket, DatabaseProvider databaseProvider) {
        this.socket = socket;
        this.databaseProvider = databaseProvider;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());
        Packager packager = null;
        try {
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch(IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        Executor exe = new Executor(databaseProvider);
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                ExecResult execResult = exe.execute(sql);
                // 将 execResult 编码成字节数组 byte[]
                result = ExecResultCodec.encode(execResult);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
