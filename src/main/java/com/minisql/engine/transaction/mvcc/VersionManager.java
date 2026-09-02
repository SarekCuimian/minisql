package com.minisql.engine.transaction.mvcc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.storage.record.PageRecord;
import com.minisql.engine.storage.wal.LogRecord;
import com.minisql.engine.storage.wal.TransactionLogManager;
import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.transaction.xid.XidStatusTable;
import com.minisql.error.Error;

public final class VersionManager {

    XidAllocator xidAllocator;
    XidStatusTable xidStatusTable;
    PageRecordManager pageRecordManager;
    TransactionLogManager transactionLogManager;
    Map<Long, RuntimeTransaction> runtimeTransactionMap;
    Lock lock;
    LockManager lockManager;
    /** 负数 XID 只在当前进程内标识只读事务，永远不会写入记录、XID 文件或 WAL。 */
    private final AtomicLong nextReadOnlyXid = new AtomicLong(-1);

    public VersionManager(
            XidAllocator xidAllocator,
            XidStatusTable xidStatusTable,
            PageRecordManager pageRecordManager,
            TransactionLogManager transactionLogManager
    ) {
        this.xidAllocator = xidAllocator;
        this.xidStatusTable = xidStatusTable;
        this.pageRecordManager = pageRecordManager;
        this.transactionLogManager = transactionLogManager;
        this.runtimeTransactionMap = new HashMap<>();
        // 创建超级事务 xid = 0
        runtimeTransactionMap.put(
                XidAllocator.SYSTEM_XID,
                RuntimeTransaction.newTransaction(
                        XidAllocator.SYSTEM_XID,
                        IsolationLevel.defaultLevel(),
                        null
                )
        );
        this.lock = new ReentrantLock();
        this.lockManager = new LockManager();
    }

    public LockManager getLockManager() {
        return lockManager;
    }
    public ReadView openReadView(long xid) throws Exception {
        lock.lock();
        try {
            RuntimeTransaction transaction = requireTransactionLocked(xid);
            if (transaction.level == IsolationLevel.REPEATABLE_READ) {
                return transaction.readView;
            }
            return captureReadViewLocked(xid);
        } finally {
            lock.unlock();
        }
    }

    public byte[] read(long xid, ReadView readView, long uid) throws Exception {
        requireTransaction(xid);
        requireReadViewOwner(xid, readView);

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
            if(Visibility.isVisible(xidStatusTable, readView, entry)) {
                return entry.readPayload();
            } else {
                return null;
            }
        } finally {
            entry.close();
        }
    }

    public byte[] readSystem(long uid) throws Exception {
        return read(
                XidAllocator.SYSTEM_XID,
                ReadView.system(xidAllocator.peekNextXid()),
                uid
        );
    }

    public long insert(long xid, byte[] payload) throws Exception {
        requireWritableTransaction(xid);

        byte[] entryBytes = Entry.newEntryBytes(xid, payload);
        return pageRecordManager.insert(xid, entryBytes);
    }
    public boolean delete(long xid, ReadView readView, long uid) throws Exception {
        RuntimeTransaction tx = requireWritableTransaction(xid);
        requireReadViewOwner(xid, readView);

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

            if (rejectStaleVersionForWrite(tx, readView, entry)) {
                return false;
            }
            if(!Visibility.isVisible(xidStatusTable, readView, entry)) {
                // 严格 2PL：即使最终不修改该记录，已经获得的排他锁也保留到事务结束。
                return false;
            }
            // 从这里往下，说明当前 xid 已经拿到了 uid 的"删除权"

            // xmax 等于当前事务 xid，即当前事务做的删除，返回 false
            if(entry.getXmax() == xid) {
                return false;
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
    public byte[] readForUpdate(long xid, ReadView readView, long uid) throws Exception {
        RuntimeTransaction tx = requireWritableTransaction(xid);
        requireReadViewOwner(xid, readView);

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

            if (rejectStaleVersionForWrite(tx, readView, entry)) {
                return null;
            }
            if(!Visibility.isVisible(xidStatusTable, readView, entry)) {
                // 严格 2PL：禁止事务执行期间单独释放已经获得的记录锁。
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
    public long begin(IsolationLevel level) {
        // 整个过程是原子的
        // 获取xid → 创建新事务 → 把新事务放进已激活事务map
        lock.lock();
        try {
            long xid = xidAllocator.allocate();
            xidStatusTable.recordInProgress(xid);
            try {
                transactionLogManager.begin(xid);
                ReadView transactionReadView =
                        level == IsolationLevel.REPEATABLE_READ
                                ? captureReadViewLocked(xid)
                                : null;
                RuntimeTransaction tx = RuntimeTransaction.newTransaction(
                        xid,
                        level,
                        transactionReadView
                );
                runtimeTransactionMap.put(xid, tx);
                return xid;
            } catch (RuntimeException exception) {
                xidStatusTable.recordAborted(xid);
                throw exception;
            }
        } finally {
            lock.unlock();
        }
    }
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
            runtimeTransactionMap.put(
                    xid,
                    RuntimeTransaction.newReadOnlyTransaction(xid)
            );
            return xid;
        } finally {
            lock.unlock();
        }
    }
    public void endReadOnly(long xid) throws Exception {
        lock.lock();
        try {
            RuntimeTransaction transaction = runtimeTransactionMap.get(xid);
            if (transaction == null || !transaction.readOnly) {
                throw Error.NoTransactionException;
            }
            if (!transaction.isActive()) {
                throw Error.TransactionTerminatedException;
            }
            transaction.state = RuntimeTransaction.State.COMMITTED;
            runtimeTransactionMap.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    public void commit(long xid) throws Exception {
        RuntimeTransaction transaction;
        lock.lock();
        try {
            transaction = runtimeTransactionMap.get(xid);
            if (transaction == null) {
                throw Error.NoTransactionException;
            }
            if (transaction.error != null) {
                throw transaction.error;
            }
            if (!transaction.isActive()) {
                throw Error.TransactionTerminatedException;
            }
            transaction.state = RuntimeTransaction.State.COMMITTING;
        } finally {
            lock.unlock();
        }

        // COMMIT WAL durable 是提交点。在此之前事务仍在活跃事务表中，
        // 并继续持有全部记录锁。
        LogRecord commitRecord = transactionLogManager.appendCommit(xid);
        transactionLogManager.flush(commitRecord.getEndLsn());

        // 与 BEGIN/RR 快照创建共用同一把锁，使其他事务只能看到：
        // 1) IN_PROGRESS + 仍在活跃表；或
        // 2) COMMITTED + 已从活跃表移除。
        lock.lock();
        try {
            xidStatusTable.recordCommitted(xid);
            transaction.state = RuntimeTransaction.State.COMMITTED;
            runtimeTransactionMap.remove(xid);
        } finally {
            lock.unlock();
        }

        try {
            // END 只是 recovery 清理标记，不是提交点。即使追加失败，
            // durable COMMIT 仍决定事务已经提交。
            transactionLogManager.complete(xid);
        } finally {
            lockManager.releaseAll(xid);
        }
    }

    public void abort(long xid) {
        internalAbort(xid, false);
    }

    private void internalAbort(long xid, boolean autoAborted) {
        RuntimeTransaction transaction;
        lock.lock();
        try {
            transaction = runtimeTransactionMap.get(xid);
            if (transaction == null) {
                return;
            }
            if (transaction.state == RuntimeTransaction.State.ABORTING
                    || transaction.state == RuntimeTransaction.State.ABORTED) {
                return;
            }
            if (transaction.state != RuntimeTransaction.State.ACTIVE) {
                throw new IllegalStateException(
                        "Cannot abort transaction in state "
                                + transaction.state + ": " + xid
                );
            }
            transaction.state = RuntimeTransaction.State.ABORTING;
            transaction.autoAborted |= autoAborted;
        } finally {
            lock.unlock();
        }

        // 只取消尚未获得的等待请求；已经持有的记录锁必须覆盖整个 Undo。
        lockManager.cancelWait(xid);

        if (transaction.readOnly) {
            finishAbort(transaction);
            return;
        }

        LogRecord abortRecord = transactionLogManager.appendAbort(xid);
        transactionLogManager.undo(xid, abortRecord);
        finishAbort(transaction);
    }

    private void finishAbort(RuntimeTransaction transaction) {
        lock.lock();
        try {
            transaction.state = RuntimeTransaction.State.ABORTED;
            runtimeTransactionMap.remove(transaction.xid);
        } finally {
            lock.unlock();
        }
        lockManager.releaseAll(transaction.xid);
    }

    private Entry loadEntry(long uid) throws Exception {
        PageRecord record = pageRecordManager.acquire(uid);
        if(record == null) {
            throw Error.NullEntryException;
        }
        return new Entry(record, uid);
    }

    private RuntimeTransaction requireTransaction(long xid) throws Exception {
        lock.lock();
        try {
            return requireTransactionLocked(xid);
        } finally {
            lock.unlock();
        }
    }

    private RuntimeTransaction requireTransactionLocked(long xid)
            throws Exception {
        RuntimeTransaction transaction = runtimeTransactionMap.get(xid);
        if (transaction == null) {
            throw Error.NoTransactionException;
        }
        if (transaction.error != null) {
            throw transaction.error;
        }
        if (!transaction.isActive()) {
            throw Error.TransactionTerminatedException;
        }
        return transaction;
    }

    private ReadView captureReadViewLocked(long ownerXid) {
        Set<Long> activeXids = new HashSet<>();
        for (RuntimeTransaction transaction
                : runtimeTransactionMap.values()) {
            if (transaction.xid > XidAllocator.SYSTEM_XID
                    && transaction.xid != ownerXid) {
                activeXids.add(transaction.xid);
            }
        }
        return new ReadView(
                ownerXid,
                xidAllocator.peekNextXid(),
                activeXids
        );
    }

    private static void requireReadViewOwner(long xid, ReadView readView) {
        if (readView == null) {
            throw new IllegalArgumentException("readView must not be null");
        }
        if (readView.getOwnerXid() != xid) {
            throw new IllegalArgumentException(
                    "ReadView owner XID mismatch: expected "
                            + xid + " but found "
                            + readView.getOwnerXid()
            );
        }
    }

    private RuntimeTransaction requireWritableTransaction(long xid) throws Exception {
        RuntimeTransaction transaction = requireTransaction(xid);
        if (transaction.readOnly) {
            throw Error.ReadOnlyTransactionException;
        }
        return transaction;
    }

    /**
     * 写操作拿到记录锁后重检版本，防止等待期间另一事务已经提交删除。
     *
     * <p>RC 跳过该过期物理版本；RR 按并发更新冲突回滚。普通一致性读仍严格使用
     * 原 ReadView，因此不会改变 SELECT 的快照语义。</p>
     */
    private boolean rejectStaleVersionForWrite(
            RuntimeTransaction transaction,
            ReadView readView,
            Entry entry
    ) throws Exception {
        if (!Visibility.isVersionConflict(
                xidStatusTable,
                readView,
                entry
        )) {
            return false;
        }
        if (transaction.level == IsolationLevel.READ_COMMITTED) {
            return true;
        }

        transaction.error = Error.ConcurrentUpdateException;
        transaction.autoAborted = true;
        internalAbort(transaction.xid, true);
        throw transaction.error;
    }

    /**
     * 获取记录锁
     */
    private void lockRow(RuntimeTransaction tx, long uid) throws Exception {
        CountDownLatch latch = null;
        long xid = tx.xid;
        try {
            latch = lockManager.acquire(xid, uid);
        } catch(Exception e) {
            // 死锁等情况
            tx.error = Error.ConcurrentUpdateException;
            tx.autoAborted = true;
            internalAbort(xid, true);
            throw tx.error;
        }

        // 等待前检查，防止死事务去等锁
        if (!tx.isActive()) {
            throw tx.error != null ? tx.error : Error.TransactionTerminatedException;
        }
        // 需要等待，阻塞在这里，直到别的事务把资源让给我
        if (latch != null) {
            boolean acquired = latch.await(30, TimeUnit.SECONDS);
            // await 返回后第一时间检查，有可能是锁等待超时异常或事务被终止异常
            if (!tx.isActive()) {
                throw tx.error != null ? tx.error : Error.TransactionTerminatedException;
            }
            if (!acquired) {
                tx.error = Error.LockWaitTimeoutException;
                tx.autoAborted = true;
                internalAbort(xid, true);
                throw tx.error;
            }
        }
    }

}
