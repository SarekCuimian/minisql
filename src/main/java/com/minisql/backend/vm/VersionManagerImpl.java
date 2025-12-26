package com.minisql.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.common.AbstractCache;
import com.minisql.backend.dm.DataManager;
import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.txm.TransactionManagerImpl;
import com.minisql.common.Error;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager txm;
    DataManager dm;
    Map<Long, Transaction> activeTransactionMap;
    Lock lock;
    LockManager lockManager;

    public VersionManagerImpl(TransactionManager txm, DataManager dm) {
        super(0);
        this.txm = txm;
        this.dm = dm;
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
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();
        if(tx == null) throw Error.NoTransactionException;
        if(tx.err != null) throw tx.err;

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(txm, tx, entry)) {
                return entry.getData();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();
        if(tx == null) throw Error.NoTransactionException;
        if(tx.err != null) throw tx.err;

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();
        if(tx == null) throw Error.NoTransactionException;
        if(tx.err != null) throw tx.err;

        Entry entry = null;
        try {
            entry = super.get(uid);
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
                tx.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                tx.autoAborted = true;
                throw tx.err;
            }
            // 设置 xmax 实现逻辑删除
            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }
    
    /**
     * 仅用来更新表前后指针
     */
    @Override
    public void update(long xid, long uid, byte[] data) throws Exception {
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();
        if(tx == null) throw Error.NoTransactionException;
        if(tx.err != null) throw tx.err;

        Entry entry = null;
        entry = super.get(uid);
        try {
            entry.setData(data, xid);
        } finally {
            entry.release();
        }
    }

    @Override
    public byte[] readForUpdate(long xid, long uid) throws Exception {
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();
        if(tx == null) throw Error.NoTransactionException;
        if(tx.err != null) throw tx.err;

        Entry entry = null;
        try {
            entry = super.get(uid);
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

            return entry.getData();
        } finally {
            entry.release();
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
            long xid = txm.begin();
            Transaction tx = Transaction.newTransaction(xid, level, activeTransactionMap);
            activeTransactionMap.put(xid, tx);
            return xid;
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
            if (tx.err != null) throw tx.err;
            tx.terminated = true;
            activeTransactionMap.remove(xid);
        } finally {
            lock.unlock();
        }
        lockManager.clear(xid);
        long lsn = txm.getLastLsn(xid);
        dm.flushLog(lsn);
        txm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
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
        txm.abort(xid);
    }


    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry loadCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void flushCache(Entry entry) {
        entry.releaseDataItem();
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
            tx.err = Error.ConcurrentUpdateException;
            internAbort(xid, true);
            tx.autoAborted = true;
            throw tx.err;
        }

        // 等待前检查，防止死事务去等锁
        if (tx.terminated) {
            throw tx.err != null ? tx.err : Error.TransactionTerminatedException;
        }
        // 需要等待，阻塞在这里，直到别的事务把资源让给我
        if (latch != null) {
            boolean acquired = latch.await(30, TimeUnit.SECONDS);
            // await 返回后第一时间检查，有可能是锁等待超时异常或事务被终止异常
            if (tx.terminated) {
                throw tx.err != null ? tx.err : Error.TransactionTerminatedException;
            }
            if (!acquired) {
                tx.err = Error.LockWaitTimeoutException;
                internAbort(xid, true);
                tx.autoAborted = true;
                throw tx.err;
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
