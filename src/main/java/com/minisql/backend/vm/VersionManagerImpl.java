package com.minisql.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.common.AbstractCache;
import com.minisql.backend.dm.DataManager;
import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.txm.TransactionManagerImpl;
import com.minisql.backend.utils.Panic;
import com.minisql.common.Error;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager txm;
    DataManager dm;
    Map<Long, Transaction> activeTransactionMap;
    Lock lock;
    LockTable lockTable;

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
        this.lockTable = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();

        if(tx.err != null) {
            throw tx.err;
        }

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
                return entry.data();
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

        if(tx.err != null) {
            throw tx.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();

        if(tx.err != null) {
            throw tx.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            if(!Visibility.isVisible(txm, tx, entry)) {
                return false;
            }
            CountDownLatch latch = null;
            try {
                latch = lockTable.add(xid, uid);
            } catch(Exception e) {
                // 死锁等情况
                tx.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                tx.autoAborted = true;
                throw tx.err;
            }

            // 需要等待，阻塞在这里，直到别的事务把资源让给我
            if (latch != null) {
                latch.await();   // delete 已经 throws Exception，所以这里直接抛出去即可
            }

            // 从这里往下，说明当前 xid 已经拿到了 uid 的"删除权"
            if(entry.getXmax() == xid) {
                return false;
            }

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
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        lock.unlock();

        try {
            if(tx.err != null) {
                throw tx.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransactionMap.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransactionMap.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        txm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction tx = activeTransactionMap.get(xid);
        if(!autoAborted) {
            activeTransactionMap.remove(xid);
        }
        lock.unlock();

        if(tx.autoAborted) return;
        lockTable.remove(xid);
        txm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
    
}
