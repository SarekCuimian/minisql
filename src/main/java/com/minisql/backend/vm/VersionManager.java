package com.minisql.backend.vm;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.txm.TransactionManager;

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(IsolationLevel level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager txm, DataManager dm) {
        return new VersionManagerImpl(txm, dm);
    }

}
