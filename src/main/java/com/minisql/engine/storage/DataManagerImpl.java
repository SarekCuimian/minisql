package com.minisql.engine.storage;

import com.minisql.engine.cache.AbstractCache;
import com.minisql.engine.storage.record.DataItem;
import com.minisql.engine.storage.record.DataItemImpl;
import com.minisql.engine.storage.recovery.Recover;
import com.minisql.engine.storage.wal.LogManager;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.PageOne;
import com.minisql.engine.storage.page.PageX;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.storage.page.fsm.FreeSpaceMap;
import com.minisql.engine.storage.page.fsm.FreeSpace;
import com.minisql.engine.transaction.status.TransactionManager;
import com.minisql.error.Panic;
import com.minisql.engine.storage.codec.UidUtil;
import com.minisql.error.Error;

import java.util.Map;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager txm;
    PageCache pageCache;
    LogManager logManager;
    FreeSpaceMap fsm;
    Page pageOne;

    public DataManagerImpl(PageCache pageCache, LogManager logManager, TransactionManager txm) {
        super(0);
        this.pageCache = pageCache;
        this.logManager = logManager;
        this.txm = txm;
        this.fsm = new FreeSpaceMap();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE_SIZE) {
            throw Error.DataTooLargeException;
        }

        FreeSpace freeSpace = null;
        // 尝试从 FSM 中获取一个满足要求的空闲页空间
        for(int i = 0; i < 5; i ++) {
            freeSpace = fsm.poll(raw.length);
            if (freeSpace != null) {
                break;
            } else {
                int newPgno = pageCache.newPage(PageX.initRaw());
                fsm.add(newPgno, PageX.MAX_FREE_SPACE_SIZE);
            }
        }
        if(freeSpace == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpaceSize = freeSpace.size;
        try {
            pg = pageCache.getPage(freeSpace.pgno);
            pg.wLock();
            try {
                // 把一次更新操作序列化成 WAL payload
                byte[] payload = Recover.insertLog(xid, pg, raw);
                // 写入日志缓冲区，并返回该条日志的 LSN 边界
                long[] pos = logManager.log(payload);
                long startLsn = pos[LogManager.POS_START];
                long endLsn = pos[LogManager.POS_END];
                // 更新当前事务的最新 LSN
                txm.updateLastLsn(xid, endLsn);
                // 在 dpt 中记录脏页与首次变脏 recLsn
                pageCache.markDirtyPage(pg.getPageNumber(), startLsn);
                short offset = PageX.insert(pg, raw);
                // 内存页更新后，更新当前页的 LSN
                pg.setPageLsn(endLsn);

                freeSpaceSize = PageX.getFreeSpaceSize(pg);
                return UidUtil.getUid(freeSpace.pgno, offset);
            } finally {
                pg.wUnlock();
            }

        } finally {
            // 将取出的pg重新插入FSM
            fsm.add(freeSpace.pgno, freeSpaceSize);
            if(pg != null) {
                pg.release();
            }
        }
    }

    @Override
    public void close() {
        super.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pageCache.close();
        logManager.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] payLoad = Recover.updateLog(xid, di);
        long[] pos = logManager.log(payLoad);
        long startLsn = pos[LogManager.POS_START];
        long endLsn = pos[LogManager.POS_END];
        txm.updateLastLsn(xid, endLsn);
        Page pg = di.getPage();
        pg.wLock();
        try {
            pageCache.markDirtyPage(pg.getPageNumber(), startLsn);
            pg.setPageLsn(endLsn);
        } finally {
            pg.wUnlock();
        }
    }

    @Override
    public void flushLog(long lsn) {
        if (lsn <= 0) return;
        logManager.flush(lsn);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem loadCache(long uid) throws Exception {
        short offset = UidUtil.getOffset(uid);
        int pgno = UidUtil.getPgno(uid);
        Page pg = pageCache.getPage(pgno);
        try {
            pg.rLock();
            try {
                return DataItem.parseDataItem(pg, offset, this);
            } finally {
                pg.rUnlock();
            }
        } catch (RuntimeException e) {
            pg.release();
            throw e;
        }
    }

    @Override
    protected void flushCache(DataItem di) {
        di.getPage().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pageCache.newPage(PageOne.initRaw());
        assert pgno == 1;
        try {
            pageOne = pageCache.getPage(pgno);
        } catch (Exception e) {
            Panic.of(e);
        }
        pageCache.persistPageOne(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean checkPageOne() {
        try {
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.of(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化 FreeSpaceMap
     */
    void initFreeSpaceMap() {
        Map<Integer, Integer> freeSpaceMap = pageCache.getPageFreeMap();
        for (Map.Entry<Integer, Integer> entry : freeSpaceMap.entrySet()) {
            fsm.add(entry.getKey(), entry.getValue());
        }
    }
    
}
