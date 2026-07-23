package com.minisql.engine.storage.record;

import java.util.Map;

import com.minisql.engine.storage.codec.UidUtil;
import com.minisql.engine.storage.page.DataPage;
import com.minisql.engine.storage.page.MetaPage;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.storage.page.fsm.FreeSpace;
import com.minisql.engine.storage.page.fsm.FreeSpaceMap;
import com.minisql.engine.storage.wal.Recovery;
import com.minisql.engine.storage.wal.LogManager;
import com.minisql.engine.transaction.status.TransactionManager;
import com.minisql.error.Error;
import com.minisql.error.Panic;

/** 协调页内 record 的存取、WAL 与空闲空间管理。 */
public final class RecordManager implements AutoCloseable {

    private final TransactionManager transactionManager;
    private final PageCache pageCache;
    private final LogManager logManager;
    private final FreeSpaceMap freeSpaceMap;
    private Page metaPage;

    private RecordManager(PageCache pageCache, LogManager logManager, TransactionManager transactionManager) {
        this.pageCache = pageCache;
        this.logManager = logManager;
        this.transactionManager = transactionManager;
        this.freeSpaceMap = new FreeSpaceMap();
    }

    public static RecordManager create(String path, long memory, TransactionManager transactionManager) {
        LogManager logManager = LogManager.create(path);
        PageCache pageCache = PageCache.create(path, memory);
        pageCache.setLogManager(logManager);

        RecordManager recordManager = new RecordManager(pageCache, logManager, transactionManager);
        recordManager.initializeMetaPage();
        return recordManager;
    }

    public static RecordManager open(String path, long memory, TransactionManager transactionManager) {
        LogManager logManager = LogManager.open(path);
        PageCache pageCache = PageCache.open(path, memory);
        pageCache.setLogManager(logManager);

        RecordManager recordManager = new RecordManager(pageCache, logManager, transactionManager);
        if (!recordManager.loadAndCheckMetaPage()) {
            Recovery.recover(transactionManager, logManager, pageCache);
        }
        recordManager.initializeFreeSpaceMap();
        MetaPage.setVcOpen(recordManager.metaPage);
        pageCache.persistPageOne(recordManager.metaPage);
        return recordManager;
    }

    public PageRecord acquire(long uid) throws Exception {
        short offset = UidUtil.getOffset(uid);
        int pageNumber = UidUtil.getPgno(uid);
        Page page = pageCache.getPage(pageNumber);
        try {
            page.rLock();
            try {
                // 解析 record header 与检查 valid flag 必须是同一次 Page 读取视图。
                PageRecord record = PageRecord.parse(page, offset, this);
                if (!record.isValid()) {
                    record.close();
                    return null;
                }
                return record;
            } finally {
                page.rUnlock();
            }
        } catch (RuntimeException e) {
            page.release();
            throw e;
        }
    }

    public long insert(long xid, byte[] payload) throws Exception {
        byte[] recordBytes = PageRecord.newRecordBytes(payload);
        if (recordBytes.length > DataPage.MAX_FREE_SPACE_SIZE) {
            throw Error.DataTooLargeException;
        }

        FreeSpace freeSpace = null;
        for (int i = 0; i < 5; i++) {
            freeSpace = freeSpaceMap.poll(recordBytes.length);
            if (freeSpace != null) {
                break;
            }
            int pageNumber = pageCache.newPage(DataPage.newPageBytes());
            freeSpaceMap.add(pageNumber, DataPage.MAX_FREE_SPACE_SIZE);
        }
        if (freeSpace == null) {
            throw Error.DatabaseBusyException;
        }

        Page page = null;
        int freeSpaceSize = freeSpace.size;
        try {
            page = pageCache.getPage(freeSpace.pgno);
            page.wLock();
            try {
                byte[] logPayload = Recovery.newInsertLogBytes(xid, page, recordBytes);
                long[] positions = logManager.log(logPayload);
                long startLsn = positions[LogManager.START_LSN_INDEX];
                long endLsn = positions[LogManager.END_LSN_INDEX];
                transactionManager.updateLastLsn(xid, endLsn);
                pageCache.markDirtyPage(page.getPageNumber(), startLsn);

                short offset = DataPage.insert(page, recordBytes);
                page.setPageLsn(endLsn);
                freeSpaceSize = DataPage.getFreeSpaceSize(page);
                return UidUtil.getUid(freeSpace.pgno, offset);
            } finally {
                page.wUnlock();
            }
        } finally {
            freeSpaceMap.add(freeSpace.pgno, freeSpaceSize);
            if (page != null) {
                page.release();
            }
        }
    }

    /** 为已完成的 record 修改写入 WAL，并更新 Page LSN。 */
    public void logRecordUpdate(long xid, PageRecord record) {
        byte[] logPayload = Recovery.newUpdateLogBytes(xid, record);
        long[] positions = logManager.log(logPayload);
        long startLsn = positions[LogManager.START_LSN_INDEX];
        long endLsn = positions[LogManager.END_LSN_INDEX];
        transactionManager.updateLastLsn(xid, endLsn);

        Page page = record.getPage();
        page.wLock();
        try {
            pageCache.markDirtyPage(page.getPageNumber(), startLsn);
            page.setPageLsn(endLsn);
        } finally {
            page.wUnlock();
        }
    }

    public void flushLog(long lsn) {
        if (lsn > 0) {
            logManager.flush(lsn);
        }
    }

    @Override
    public void close() {
        MetaPage.setVcClose(metaPage);
        metaPage.release();
        pageCache.close();
        logManager.close();
    }

    private void initializeMetaPage() {
        int pageNumber = pageCache.newPage(MetaPage.newPageBytes());
        assert pageNumber == 1;
        try {
            metaPage = pageCache.getPage(pageNumber);
        } catch (Exception e) {
            Panic.of(e);
        }
        pageCache.persistPageOne(metaPage);
    }

    private boolean loadAndCheckMetaPage() {
        try {
            metaPage = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.of(e);
        }
        return MetaPage.checkVc(metaPage);
    }

    private void initializeFreeSpaceMap() {
        Map<Integer, Integer> freeSpaceByPage = pageCache.getPageFreeMap();
        for (Map.Entry<Integer, Integer> entry : freeSpaceByPage.entrySet()) {
            freeSpaceMap.add(entry.getKey(), entry.getValue());
        }
    }
}
