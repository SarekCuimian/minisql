package com.minisql.backend.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.minisql.backend.dbm.DatabaseManager;
import com.minisql.common.ExecResult;
import com.minisql.common.ExecResultCodec;
import com.minisql.transport.Encoder;
import com.minisql.transport.Package;
import com.minisql.transport.Packager;
import com.minisql.transport.Transporter;

public class Server {
    private int port;
    private final DatabaseManager databaseManager;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ServerSocket serverSocket;
    private ThreadPoolExecutor tpe;

    public Server(int port, DatabaseManager databaseManager) {
        this.port = port;
        this.databaseManager = databaseManager;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);

        // 线程池
        tpe = new ThreadPoolExecutor(
            10, 
            20, 
            1L, 
            TimeUnit.SECONDS, 
            new ArrayBlockingQueue<>(100), 
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        try {
            while(running.get()) {
                Socket socket = serverSocket.accept();
                Runnable handleSocket = new HandleSocket(socket, databaseManager);
                tpe.execute(handleSocket);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            stop();
        }
    }

    public void stop() {
        if(!running.compareAndSet(true, false)) {
            return;
        }
        try {
            if(serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        if(tpe != null) {
            tpe.shutdown();
            try {
                if(!tpe.awaitTermination(5, TimeUnit.SECONDS)) {
                    tpe.shutdownNow();
                }
            } catch (InterruptedException ie) {
                tpe.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        databaseManager.shutdown();
    }
}

class HandleSocket implements Runnable {
    private Socket socket;
    private DatabaseManager databaseManager;

    public HandleSocket(Socket socket, DatabaseManager databaseManager) {
        this.socket = socket;
        this.databaseManager = databaseManager;
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
        Executor exe = new Executor(databaseManager);
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
