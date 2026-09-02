package com.minisql.engine.storage.wal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 恢复与 checkpoint 共享的活跃事务表：XID → status + lastLsn。
 */
public final class ActiveTransactionTable {

    private final Lock lock = new ReentrantLock();
    private final Map<Long, ActiveTransaction> transactions = new HashMap<>();

    public void add(long xid, ActiveTransaction.Status status, long lastLsn) {
        requireXid(xid);
        requireLsn(lastLsn);
        lock.lock();
        try {
            transactions.put(xid, new ActiveTransaction(xid, status, lastLsn));
        } finally {
            lock.unlock();
        }
    }

    public void update(long xid, ActiveTransaction.Status status, long lastLsn) {
        add(xid, status, lastLsn);
    }

    public ActiveTransaction get(long xid) {
        lock.lock();
        try {
            return transactions.get(xid);
        } finally {
            lock.unlock();
        }
    }

    public void remove(long xid) {
        lock.lock();
        try {
            transactions.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    public Map<Long, ActiveTransaction> snapshot() {
        lock.lock();
        try {
            return Map.copyOf(transactions);
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return transactions.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    private static void requireXid(long xid) {
        if (xid <= 0) {
            throw new IllegalArgumentException("xid must be positive: " + xid);
        }
    }

    private static void requireLsn(long lsn) {
        if (lsn < LogRecord.NO_LSN) {
            throw new IllegalArgumentException("lastLsn must not be negative: " + lsn);
        }
    }

}
