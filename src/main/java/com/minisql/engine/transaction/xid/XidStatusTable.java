package com.minisql.engine.transaction.xid;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 持久化维护 {@code xid -> execution result}，供 MVCC 与恢复查询。
 *
 * <p>WAL 是崩溃恢复时的提交权威；本表提供运行期快速查询，并允许恢复过程
 * 修复尚未及时落盘的状态。</p>
 */
public final class XidStatusTable {

    private final XidAllocator xidAllocator;
    private final Lock readLock;
    private final Lock writeLock;

    public XidStatusTable(XidAllocator xidAllocator) {
        this.xidAllocator = Objects.requireNonNull(
                xidAllocator,
                "xidAllocator must not be null"
        );
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    public void recordInProgress(long xid) {
        transition(xid, XidAllocator.STATUS_IN_PROGRESS);
    }

    public void recordCommitted(long xid) {
        transition(xid, XidAllocator.STATUS_COMMITTED);
    }

    public void recordAborted(long xid) {
        transition(xid, XidAllocator.STATUS_ABORTED);
    }

    public boolean isInProgress(long xid) {
        if (xid == XidAllocator.SYSTEM_XID) {
            return false;
        }
        return hasStatus(xid, XidAllocator.STATUS_IN_PROGRESS);
    }

    public boolean isCommitted(long xid) {
        if (xid == XidAllocator.SYSTEM_XID) {
            return true;
        }
        return hasStatus(xid, XidAllocator.STATUS_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if (xid == XidAllocator.SYSTEM_XID) {
            return false;
        }
        return hasStatus(xid, XidAllocator.STATUS_ABORTED);
    }

    private boolean hasStatus(long xid, byte expected) {
        readLock.lock();
        try {
            return xidAllocator.readStatus(xid) == expected;
        } finally {
            readLock.unlock();
        }
    }

    private void transition(long xid, byte target) {
        if (xid <= XidAllocator.SYSTEM_XID) {
            throw new IllegalArgumentException("XID must be positive: " + xid);
        }
        writeLock.lock();
        try {
            byte current = xidAllocator.readStatus(xid);
            if (current == target) {
                return;
            }
            if (!isAllowedTransition(current, target)) {
                throw new IllegalStateException(
                        "Illegal XID status transition "
                                + statusName(current) + " -> " + statusName(target)
                                + " for XID " + xid
                );
            }
            xidAllocator.writeStatus(xid, target);
        } finally {
            writeLock.unlock();
        }
    }

    private static boolean isAllowedTransition(byte current, byte target) {
        if (target == XidAllocator.STATUS_IN_PROGRESS) {
            return current == XidAllocator.STATUS_UNUSED;
        }
        if (target == XidAllocator.STATUS_COMMITTED) {
            return current == XidAllocator.STATUS_UNUSED
                    || current == XidAllocator.STATUS_IN_PROGRESS;
        }
        if (target == XidAllocator.STATUS_ABORTED) {
            return current == XidAllocator.STATUS_UNUSED
                    || current == XidAllocator.STATUS_IN_PROGRESS;
        }
        return false;
    }

    private static String statusName(byte status) {
        switch (status) {
            case XidAllocator.STATUS_UNUSED:
                return "UNUSED";
            case XidAllocator.STATUS_IN_PROGRESS:
                return "IN_PROGRESS";
            case XidAllocator.STATUS_COMMITTED:
                return "COMMITTED";
            case XidAllocator.STATUS_ABORTED:
                return "ABORTED";
            default:
                return "UNKNOWN(" + Byte.toUnsignedInt(status) + ")";
        }
    }
}
