package com.minisql.engine.transaction.mvcc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.storage.record.PageRecord;
import com.minisql.engine.storage.wal.LogRecord;
import com.minisql.engine.transaction.status.TransactionManager;
import com.minisql.engine.transaction.status.TransactionManagerImpl;
import com.minisql.error.Error;

public class VersionManagerImpl implements VersionManager {

    TransactionManager txm;
    PageRecordManager pageRecordManager;
    Map<Long, Transaction> activeTransactionMap;
    Lock lock;
    LockManager lockManager;
    /** 负数 XID 只在当前进程内标识只读事务，永远不会写入记录、XID 文件或 WAL。 */
    private final AtomicLong nextReadOnlyXid = new AtomicLong(-1);

    public VersionManagerImpl(TransactionManager txm, PageRecordManager pageRecordManager) {
        this.txm = txm;
        this.pageRecordManager = pageRecordManager;
        this.activeTransactionMap = new HashMap<>();
        // 创建超级事务 xid = 0
        activeTransactionMap.put(
                TransactionManagerImpl.SUPER_XID,
                Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, IsolationLevel.defaultLevel(), null)
        );
        this.lock = new ReentrantLock();
        this.lockManager = new LockManager();
    }

    @Override
    public LockManager getLockManager() {
        return lockManager;
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        Transaction tx = requireTransaction(xid);

        Entry entry = null;
        try {
            entry = loadEntry(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(txm, tx, entry)) {
                return entry.readPayload();
            } else {
                return null;
            }
        } finally {
            entry.close();
        }
    }

    @Override
    public long insert(long xid, byte[] payload) throws Exception {
        requireWritableTransaction(xid);

        byte[] entryBytes = Entry.newEntryBytes(xid, payload);
        return pageRecordManager.insert(xid, entryBytes);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        Transaction tx = requireWritableTransaction(xid);

        Entry entry = null;
        try {
            entry = loadEntry(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) return false;
            else throw e;
        }

        try {
            // 先拿锁（必要时等待），避免等待期间版本变化导致删除基于过期可见性
            lockRow(tx, uid);

            if(!Visibility.isVisible(txm, tx, entry)) {
                // 已不可见，释放该行锁，避免无意义占用
                unlockRow(tx, uid);
                return false;
            }
            // 从这里往下，说明当前 xid 已经拿到了 uid 的"删除权"

            // xmax 等于当前事务 xid，即当前事务做的删除，返回 false
            if(entry.getXmax() == xid) {
                return false;
            }

            // 发生了并发更新冲突，内部主动回滚
            if(Visibility.isVersionSkip(txm, tx, entry)) {
                tx.error = Error.ConcurrentUpdateException;
                internalAbort(xid, true);
                tx.autoAborted = true;
                throw tx.error;
            }
            // 设置 xmax 实现逻辑删除
            entry.markDeleted(xid);
            return true;

        } finally {
            entry.close();
        }
    }
    
    /**
     * 仅用来更新表前后指针
     */
    @Override
    public void update(long xid, long uid, byte[] payload) throws Exception {
        requireWritableTransaction(xid);

        Entry entry = null;
        entry = loadEntry(uid);
        try {
            entry.replacePayload(payload, xid);
        } finally {
            entry.close();
        }
    }

    @Override
    public byte[] readForUpdate(long xid, long uid) throws Exception {
        Transaction tx = requireWritableTransaction(xid);

        Entry entry = null;
        try {
            entry = loadEntry(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) return null;
            else throw e;
        }

        try {
            // 先拿锁（必要时等待），再判断可见性，避免等待期间版本变化导致读取过期版本
            lockRow(tx, uid);

            if(!Visibility.isVisible(txm, tx, entry)) {
                // 已不可见，释放该行锁，避免无意义占用
                unlockRow(tx, uid);
                return null;
            }

            return entry.readPayload();
        } finally {
            entry.close();
        }
    }


    /**
     * 开启事务
     * @param level 事务隔离级别
     * @return 事务ID
     */
    @Override
    public long begin(IsolationLevel level) {
        // 整个过程是原子的
        // 获取xid → 创建新事务 → 把新事务放进已激活事务map
        lock.lock();
        try {
            // 开启事务，获取xid
            long xid = pageRecordManager.beginTransaction();
            Transaction tx = Transaction.newTransaction(xid, level, activeTransactionMap);
            activeTransactionMap.put(xid, tx);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long beginReadOnly() {
        long xid = nextReadOnlyXid.getAndUpdate(
                current -> current == Long.MIN_VALUE
                        ? Long.MIN_VALUE
                        : current - 1
        );
        if (xid == Long.MIN_VALUE) {
            throw new IllegalStateException("read-only xid space exhausted");
        }
        lock.lock();
        try {
            activeTransactionMap.put(
                    xid,
                    Transaction.newReadOnlyTransaction(xid)
            );
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void endReadOnly(long xid) throws Exception {
        lock.lock();
        try {
            Transaction transaction = activeTransactionMap.get(xid);
            if (transaction == null || !transaction.readOnly) {
                throw Error.NoTransactionException;
            }
            transaction.terminated = true;
            activeTransactionMap.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        Transaction tx;
        lock.lock();
        try {
            tx = activeTransactionMap.get(xid);
            if (tx == null) throw Error.NoTransactionException;
            if (tx.error != null) throw tx.error;
            tx.terminated = true;
            activeTransactionMap.remove(xid);
        } finally {
            lock.unlock();
        }
        lockManager.clear(xid);
        LogRecord commitRecord = pageRecordManager.appendCommitLog(xid);
        pageRecordManager.flushLog(commitRecord.getEndLsn());
        txm.commit(xid);
        pageRecordManager.appendEndLog(xid, commitRecord.getStartLsn());
        txm.complete(xid);
    }

    @Override
    public void abort(long xid) {
        internalAbort(xid, false);
    }

    private void internalAbort(long xid, boolean autoAborted) {
        Transaction tx;
        lock.lock();
        try {
            tx = activeTransactionMap.get(xid);
            if (tx == null) return;
            tx.terminated = true;
            // 自动回滚标记，如果已经被系统标记为true，则一直保持
            tx.autoAborted |= autoAborted; 
            activeTransactionMap.remove(xid);
        } finally {
            lock.unlock();
        }
        lockManager.clear(xid);
        LogRecord abortRecord = pageRecordManager.appendAbortLog(xid);
        pageRecordManager.undoTransaction(xid, abortRecord);
    }


    private Entry loadEntry(long uid) throws Exception {
        PageRecord record = pageRecordManager.acquire(uid);
        if(record == null) {
            throw Error.NullEntryException;
        }
        return new Entry(record, uid);
    }

    private Transaction requireTransaction(long xid) throws Exception {
        Transaction transaction;
        lock.lock();
        try {
            transaction = activeTransactionMap.get(xid);
        } finally {
            lock.unlock();
        }
        if (transaction == null) {
            throw Error.NoTransactionException;
        }
        if (transaction.error != null) {
            throw transaction.error;
        }
        return transaction;
    }

    private Transaction requireWritableTransaction(long xid) throws Exception {
        Transaction transaction = requireTransaction(xid);
        if (transaction.readOnly) {
            throw Error.ReadOnlyTransactionException;
        }
        return transaction;
    }
    
    /**
     * 获取行锁
     */
    private void lockRow(Transaction tx, long uid) throws Exception {
        CountDownLatch latch = null;
        long xid = tx.xid;
        try {
            latch = lockManager.acquire(xid, uid);
        } catch(Exception e) {
            // 死锁等情况
            tx.error = Error.ConcurrentUpdateException;
            internalAbort(xid, true);
            tx.autoAborted = true;
            throw tx.error;
        }

        // 等待前检查，防止死事务去等锁
        if (tx.terminated) {
            throw tx.error != null ? tx.error : Error.TransactionTerminatedException;
        }
        // 需要等待，阻塞在这里，直到别的事务把资源让给我
        if (latch != null) {
            boolean acquired = latch.await(30, TimeUnit.SECONDS);
            // await 返回后第一时间检查，有可能是锁等待超时异常或事务被终止异常
            if (tx.terminated) {
                throw tx.error != null ? tx.error : Error.TransactionTerminatedException;
            }
            if (!acquired) {
                tx.error = Error.LockWaitTimeoutException;
                internalAbort(xid, true);
                tx.autoAborted = true;
                throw tx.error;
            }
        }
    }

    /**
     * 释放行锁
     */
    private void unlockRow(Transaction tx, long uid) {
        lockManager.release(tx.xid, uid);
    }

}
