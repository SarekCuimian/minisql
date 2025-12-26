package com.minisql.backend.dm;

import com.minisql.backend.dm.dataitem.DataItem;
import com.minisql.backend.dm.logger.LogManager;
import com.minisql.backend.dm.page.PageOne;
import com.minisql.backend.dm.page.cache.PageCache;
import com.minisql.backend.txm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();
    void flushLog(long lsn);

    static DataManager create(String path, long mem, TransactionManager txm) {
        PageCache pc = PageCache.create(path, mem);
        LogManager lgm = LogManager.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lgm, txm);
        dm.initPageOne();
        return dm;
    }

    static DataManager open(String path, long mem, TransactionManager txm) {
        PageCache pc = PageCache.open(path, mem);
        LogManager lgm = LogManager.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lgm, txm);
        if(!dm.checkPageOne()) {
            Recover.recover(txm, lgm, pc);
        }
        dm.initFreeSpaceMap();
        PageOne.setVcOpen(dm.pageOne);
        dm.pageCache.persistPage(dm.pageOne);
        return dm;
    }
}
