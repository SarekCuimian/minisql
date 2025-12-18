package com.minisql.backend.dm;

import com.minisql.backend.common.AbstractCache;
import com.minisql.backend.dm.dataitem.DataItem;
import com.minisql.backend.dm.dataitem.DataItemImpl;
import com.minisql.backend.dm.logger.Logger;
import com.minisql.backend.dm.page.Page;
import com.minisql.backend.dm.page.PageOne;
import com.minisql.backend.dm.page.PageX;
import com.minisql.backend.dm.page.cache.PageCache;
import com.minisql.backend.dm.page.fsm.FreeSpaceMap;
import com.minisql.backend.dm.page.fsm.FreeSpace;
import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.UidUtil;
import com.minisql.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager txm;
    PageCache pageCache;
    Logger logger;
    FreeSpaceMap fsm;
    Page pageOne;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager txm) {
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
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
        int freeSpaceSize = 0;
        try {
            pg = pageCache.getPage(freeSpace.pgno);
            // 把一次更新操作序列化成 WAL payload
            byte[] log = Recover.insertLog(xid, pg, raw);
            // 把 payload 包入 [Size][Checksum][Data] 并顺序写入 .log 文件
            logger.log(log);

            short offset = PageX.insert(pg, raw);
            return UidUtil.addressToUid(freeSpace.pgno, offset);

        } finally {
            // 将取出的pg重新插入FSM
            if(pg != null) {
                fsm.add(freeSpace.pgno, PageX.getFreeSpaceSize(pg));
                pg.release();
            } else {
                fsm.add(freeSpace.pgno, freeSpaceSize);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem loadCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pageCache.getPage(pgno);
        try {
            return DataItem.parseDataItem(pg, offset, this);
        } catch (RuntimeException e) {
            pg.release();
            throw e;
        }
    }

    @Override
    protected void flushCache(DataItem di) {
        di.page().release();
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
        pageCache.persistPage(pageOne);
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
        int pageCount = pageCache.getPageCount();
        for(int i = 2; i <= pageCount; i ++) {
            Page pg = null;
            try {
                pg = pageCache.getPage(i);
                fsm.add(pg.getPageNumber(), PageX.getFreeSpaceSize(pg));
            } catch (Exception e) {
                Panic.of(e);
            } finally {
                if(pg != null) {
                    pg.release();
                }
            }
        }
    }
    
}
