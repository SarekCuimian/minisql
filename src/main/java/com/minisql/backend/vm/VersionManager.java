package com.minisql.backend.vm;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.txm.TransactionManager;

public interface VersionManager {

    // 事务管理
    long begin(IsolationLevel level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    // 数据管理
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;
    void update(long xid, long uid, byte[] data) throws Exception;

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

    static VersionManager create(TransactionManager txm, DataManager dm) {
        return new VersionManagerImpl(txm, dm);
    }

}
