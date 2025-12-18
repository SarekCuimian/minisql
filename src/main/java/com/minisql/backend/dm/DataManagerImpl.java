package com.minisql.backend.dm;

import com.minisql.backend.common.AbstractCache;
import com.minisql.backend.dm.dataitem.DataItem;
import com.minisql.backend.dm.dataitem.DataItemImpl;
import com.minisql.backend.dm.logger.Logger;
import com.minisql.backend.dm.page.Page;
import com.minisql.backend.dm.page.PageOne;
import com.minisql.backend.dm.page.PageX;
import com.minisql.backend.dm.page.cache.PageCache;
import com.minisql.backend.dm.page.index.PageIndex;
import com.minisql.backend.dm.page.index.PageInfo;
import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.UidUtil;
import com.minisql.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager txm;
    PageCache cache;
    Logger logger;
    PageIndex pgIndex;
    Page pageOne;

    public DataManagerImpl(PageCache cache, Logger logger, TransactionManager txm) {
        super(0);
        this.cache = cache;
        this.logger = logger;
        this.txm = txm;
        this.pgIndex = new PageIndex();
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
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pgIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = cache.newPage(PageX.initRaw());
                pgIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = cache.getPage(pi.pgno);
            // 把一次更新操作序列化成 WAL payload
            byte[] log = Recover.insertLog(xid, pg, raw);
            // 把 payload 包入 [Size][Checksum][Data] 并顺序写入 .log 文件
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return UidUtil.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pgIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pgIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        cache.close();
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
        Page pg = cache.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void flushCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = cache.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = cache.getPage(pgno);
        } catch (Exception e) {
            Panic.of(e);
        }
        cache.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = cache.getPage(1);
        } catch (Exception e) {
            Panic.of(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = cache.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = cache.getPage(i);
                pgIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
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
