package com.minisql.engine.storage;

import com.minisql.engine.storage.record.DataItem;
import com.minisql.engine.storage.recovery.Recover;
import com.minisql.engine.storage.wal.LogManager;
import com.minisql.engine.storage.page.PageOne;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.transaction.status.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();
    void flushLog(long lsn);

    static DataManager create(String path, long mem, TransactionManager txm) {
        LogManager lgm = LogManager.create(path);
        PageCache pc = PageCache.create(path, mem);
        pc.setLogManager(lgm);
        DataManagerImpl dm = new DataManagerImpl(pc, lgm, txm);
        dm.initPageOne();
        return dm;
    }

    static DataManager open(String path, long mem, TransactionManager txm) {
        LogManager lgm = LogManager.open(path);
        PageCache pc = PageCache.open(path, mem);
        pc.setLogManager(lgm);
        DataManagerImpl dm = new DataManagerImpl(pc, lgm, txm);
        if(!dm.checkPageOne()) {
            Recover.recover(txm, lgm, pc);
        }
        dm.initFreeSpaceMap();
        PageOne.setVcOpen(dm.pageOne);
        dm.pageCache.persistPageOne(dm.pageOne);
        return dm;
    }
}
