package com.minisql.engine.transaction.mvcc;

import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.transaction.status.TransactionManager;

public interface VersionManager {

    // 事务管理
    long begin(IsolationLevel level);
    long beginReadOnly();
    void endReadOnly(long xid) throws Exception;
    void commit(long xid) throws Exception;
    void abort(long xid);

    // 数据管理
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] payload) throws Exception;
    boolean delete(long xid, long uid) throws Exception;
    void update(long xid, long uid, byte[] payload) throws Exception;

    // 锁管理
    /**
     * 获取记录并在获取期间持有写锁，确保返回的是最新可见版本。
     * 调用方在事务结束时统一释放，或显式调用 releaseRowLock 释放单条记录。
     */
    byte[] readForUpdate(long xid, long uid) throws Exception;

    /**
     * 获取锁管理器
     *
     * @return
     */
    LockManager getLockManager();

    static VersionManager create(TransactionManager txm, PageRecordManager pageRecordManager) {
        return new VersionManagerImpl(txm, pageRecordManager);
    }

}
