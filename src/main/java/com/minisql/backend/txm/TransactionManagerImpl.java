package com.minisql.backend.txm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.common.Error;

public class TransactionManagerImpl implements TransactionManager {

    // XID 文件头大小（存储 xidCounter）
    static final int XID_HEADER_SIZE = 8;

    // 每个事务状态占用 1 byte
    private static final int XID_STATUS_SIZE = 1;

    // 事务状态
    private static final byte TX_ACTIVE = 0;
    private static final byte TX_COMMITTED = 1;
    private static final byte TX_ABORTED = 2;

    // 超级事务（永远已提交）
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";

    private final RandomAccessFile file;
    private final FileChannel fc;
    private final Lock counterLock;
    private long xidCounter;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        this.counterLock = new ReentrantLock();
        checkXidCounter();
    }

    // 开始一个事务，返回新的 XID
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXidStatus(xid, TX_ACTIVE);
            incrementXidCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 提交事务
    @Override
    public void commit(long xid) {
        updateXidStatus(xid, TX_COMMITTED);
    }

    // 回滚事务
    @Override
    public void abort(long xid) {
        updateXidStatus(xid, TX_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXidStatus(xid, TX_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXidStatus(xid, TX_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXidStatus(xid, TX_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    // 校验 XID 文件合法性，并初始化 xidCounter
    private void checkXidCounter() {
        long fileLen;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.of(Error.BadXIDFileException);
            return;
        }

        if (fileLen < XID_HEADER_SIZE) {
            Panic.of(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_SIZE);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.of(e);
        }

        xidCounter = ByteUtil.parseLong(buf.array());
        long expectedLen = getXidPosition(xidCounter + 1);
        if (expectedLen != fileLen) {
            Panic.of(Error.BadXIDFileException);
        }
    }

    // 将 xidCounter +1 并持久化到文件头
    private void incrementXidCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(ByteUtil.longToByte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    // 更新指定 xid 的事务状态
    private void updateXidStatus(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[] { status });
        try {
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    // 检查指定 xid 是否处于给定状态
    private boolean checkXidStatus(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_STATUS_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.of(e);
        }
        return buf.array()[0] == status;
    }

    // 根据 xid 计算其在 xid 文件中的偏移
    private long getXidPosition(long xid) {
        return XID_HEADER_SIZE + (xid - 1) * XID_STATUS_SIZE;
    }
}
