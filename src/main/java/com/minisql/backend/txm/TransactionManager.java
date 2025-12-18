package com.minisql.backend.txm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.minisql.backend.utils.Panic;
import com.minisql.common.Error;

public interface TransactionManager {
    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();

    static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.of(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.of(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.of(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.XID_HEADER_SIZE]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.of(e);
        }
        
        return new TransactionManagerImpl(raf, fc);
    }

    static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.of(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.of(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
