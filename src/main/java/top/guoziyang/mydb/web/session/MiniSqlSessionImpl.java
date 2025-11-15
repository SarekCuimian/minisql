package top.guoziyang.mydb.web.session;

import top.guoziyang.mydb.backend.utils.format.ExecResult;
import top.guoziyang.mydb.backend.utils.format.ExecResultCodec;
import top.guoziyang.mydb.transport.Encoder;
import top.guoziyang.mydb.transport.Packager;
import top.guoziyang.mydb.transport.Transporter;
import top.guoziyang.mydb.transport.Package;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class MiniSqlSessionImpl implements MiniSqlSession {

    private final Packager packager;
    private final Socket socket;
    private final Object ioLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MiniSqlSessionImpl(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        Transporter t = new Transporter(socket);
        Encoder e = new Encoder();
        this.packager = new Packager(t, e);
    }

    @Override
    public ExecResult execute(String sql) throws Exception {
        ensureOpen();
        String statement = Objects.requireNonNull(sql, "sql must not be null").trim();
        if(statement.isEmpty()) {
            throw new IllegalArgumentException("SQL must not be empty");
        }
        synchronized (ioLock) {
            Package req = new Package(statement.getBytes(StandardCharsets.UTF_8), null);
            packager.send(req);
            Package resp = packager.receive();
            if (resp.getErr() != null) {
                throw resp.getErr();
            }
            return ExecResultCodec.decode(resp.getData());
        }
    }

    @Override
    public void close() {
        if(!closed.compareAndSet(false, true)) {
            return;
        }
        Exception closeErr = null;
        try {
            packager.close();
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
