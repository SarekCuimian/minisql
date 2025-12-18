package com.minisql.backend.dm;

import com.minisql.backend.dm.dataitem.DataItem;
import com.minisql.backend.dm.logger.Logger;
import com.minisql.backend.dm.page.PageOne;
import com.minisql.backend.dm.page.cache.PageCache;
import com.minisql.backend.txm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    static DataManager create(String path, long mem, TransactionManager txm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, txm);
        dm.initPageOne();
        return dm;
    }

    static DataManager open(String path, long mem, TransactionManager txm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, txm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(txm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.cache.flushPage(dm.pageOne);

        return dm;
    }
}
