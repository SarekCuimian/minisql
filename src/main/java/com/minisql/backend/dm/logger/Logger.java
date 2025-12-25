package com.minisql.backend.dm.logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.common.Error;

public interface Logger {
    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

    public static Logger create(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
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

        ByteBuffer buf = ByteBuffer.wrap(ByteUtil.intToByte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.of(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    public static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
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

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();

        return lg;
    }
}