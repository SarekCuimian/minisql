package com.minisql.engine.transaction.status;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.minisql.error.Panic;
import com.minisql.engine.storage.codec.ByteUtil;
import com.minisql.engine.storage.io.FileChannelUtil;
import com.minisql.error.Error;

public class TransactionManagerImpl implements TransactionManager {

    // XID 文件头大小（存储 xidCounter）
    static final int XID_HEADER_SIZE = 8;

    // 每个事务状态占用 1 byte
    private static final int XID_STATUS_SIZE = 1;

    // 事务状态
    private static final byte STATUS_ACTIVE = 0;
    private static final byte STATUS_COMMITTED = 1;
    private static final byte STATUS_ABORTED = 2;

    // 超级事务（永远已提交）
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";

    private final RandomAccessFile file;
    private final FileChannel fc;
    private final Lock rLock;
    private final Lock wLock;
    private long xidCounter;
    private final ConcurrentHashMap<Long, Long> lastLsnMap = new ConcurrentHashMap<>();

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        ReadWriteLock rwLock = new ReentrantReadWriteLock();
        this.rLock = rwLock.readLock();
        this.wLock = rwLock.writeLock();
        checkXidCounter();
    }

    // 开始一个事务，返回新的 XID
    @Override
    public long begin() {
        wLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXidStatus(xid, STATUS_ACTIVE);
            incrementXidCounter();
            return xid;
        } finally {
            wLock.unlock();
        }
    }

    // 提交事务
    @Override
    public void commit(long xid) {
        wLock.lock();
        try {
            if (xid == SUPER_XID || xid > xidCounter) {
                return;
            }
            updateXidStatus(xid, STATUS_COMMITTED);
            lastLsnMap.remove(xid);
        } finally {
            wLock.unlock();
        }
    }

    // 回滚事务
    @Override
    public void abort(long xid) {
        wLock.lock();
        try {
            if (xid == SUPER_XID || xid > xidCounter) {
                return;
            }
            updateXidStatus(xid, STATUS_ABORTED);
            lastLsnMap.remove(xid);
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        rLock.lock();
        try {
            return checkXidStatus(xid, STATUS_ACTIVE);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        rLock.lock();
        try {
            return checkXidStatus(xid, STATUS_COMMITTED);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        rLock.lock();
        try {
            return checkXidStatus(xid, STATUS_ABORTED);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void updateLastLsn(long xid, long lsn) {
        if (xid <= SUPER_XID) {
            return;
        }
        lastLsnMap.merge(xid, lsn, Math::max);
    }

    @Override
    public long getLastLsn(long xid) {
        if (xid <= SUPER_XID) {
            return 0L;
        }
        return lastLsnMap.getOrDefault(xid, 0L);
    }

    @Override
    public void close() {
        wLock.lock();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.of(e);
        } finally {
            wLock.unlock();
        }
    }

    // 校验 XID 文件合法性，并初始化 xidCounter
    private void checkXidCounter() {
        long fileSize;
        try {
            fileSize = file.length();
        } catch (IOException e) {
            Panic.of(Error.BadXIDFileException);
            return;
        }

        if (fileSize < XID_HEADER_SIZE) {
            Panic.of(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_SIZE);
        try {
            FileChannelUtil.readFully(fc, buf, 0);
        } catch (IOException e) {
            Panic.of(e);
        }

        xidCounter = ByteUtil.getLong(buf.array(), 0);
        long expectedLen = getXidPosition(xidCounter + 1);
        if (expectedLen != fileSize) {
            Panic.of(Error.BadXIDFileException);
        }
    }

    // 将 xidCounter +1 并持久化到文件头
    private void incrementXidCounter() {
        xidCounter++;
        byte[] xidCounterBytes = new byte[Long.BYTES];
        ByteUtil.putLong(xidCounterBytes, 0, xidCounter);
        ByteBuffer buf = ByteBuffer.wrap(xidCounterBytes);
        try {
            FileChannelUtil.writeFully(fc, buf, 0);
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
            FileChannelUtil.writeFully(fc, buf, offset);
            fc.force(false);
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    // 检查指定 xid 是否处于给定状态
    private boolean checkXidStatus(long xid, byte status) {
        if (xid <= 0 || xid > xidCounter) {
            return false;
        }
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.allocate(XID_STATUS_SIZE);
        try {
            FileChannelUtil.readFully(fc, buf, offset);
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
