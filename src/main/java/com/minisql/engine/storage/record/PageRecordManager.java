package com.minisql.engine.storage.record;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.codec.UidUtil;
import com.minisql.engine.storage.page.DataPage;
import com.minisql.engine.storage.page.MetaPage;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.PageBufferPool;
import com.minisql.engine.storage.page.fsm.FreeSpace;
import com.minisql.engine.storage.page.fsm.FreeSpaceMap;
import com.minisql.engine.storage.wal.WriteAheadLogManager;
import com.minisql.engine.storage.wal.LogRecord;
import com.minisql.engine.storage.wal.Recovery;
import com.minisql.engine.storage.wal.TransactionLogManager;
import com.minisql.engine.transaction.xid.XidStatusTable;
import com.minisql.error.Error;
import com.minisql.error.Panic;

/** 管理页内 PageRecord 的存取、Page LSN 与空闲空间。 */
public final class PageRecordManager implements AutoCloseable {

    private final PageBufferPool bufferPool;
    private final TransactionLogManager transactionLogManager;
    private final FreeSpaceMap freeSpaceMap;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Page metaPage;

    private PageRecordManager(PageBufferPool bufferPool, TransactionLogManager transactionLogManager) {
        this.bufferPool = java.util.Objects.requireNonNull(
                bufferPool,
                "bufferPool must not be null"
        );
        this.transactionLogManager = java.util.Objects.requireNonNull(
                transactionLogManager,
                "transactionLogManager must not be null"
        );
        this.freeSpaceMap = new FreeSpaceMap();
    }

    public static PageRecordManager create(PageBufferPool bufferPool, TransactionLogManager transactionLogManager) {
        PageRecordManager pageRecordManager = new PageRecordManager(
                bufferPool,
                transactionLogManager
        );
        pageRecordManager.initializeMetaPage();
        return pageRecordManager;
    }

    public static PageRecordManager open(
            PageBufferPool bufferPool,
            TransactionLogManager transactionLogManager,
            XidStatusTable xidStatusTable,
            WriteAheadLogManager writeAheadLogManager
    ) {
        PageRecordManager pageRecordManager = new PageRecordManager(
                bufferPool,
                transactionLogManager
        );
        if (!pageRecordManager.loadAndCheckMetaPage()) {
            Recovery.recover(xidStatusTable, writeAheadLogManager, bufferPool);
        }
        pageRecordManager.initializeFreeSpaceMap();
        MetaPage.setVcOpen(pageRecordManager.metaPage);
        bufferPool.persistMetaPage(pageRecordManager.metaPage);
        return pageRecordManager;
    }

    public PageRecord acquire(long uid) throws Exception {
        short offset = UidUtil.getOffset(uid);
        int pageNumber = UidUtil.getPgno(uid);
        Page page = bufferPool.getPage(pageNumber);
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
            int pageNumber = bufferPool.newPage(DataPage.newPageBytes());
            freeSpaceMap.add(pageNumber, DataPage.MAX_FREE_SPACE_SIZE);
        }
        if (freeSpace == null) {
            throw Error.DatabaseBusyException;
        }

        Page page = null;
        int freeSpaceSize = freeSpace.size;
        try {
            page = bufferPool.getPage(freeSpace.pgno);
            page.wLock();
            try {
                LogRecord logRecord = transactionLogManager.appendInsert(
                        xid,
                        page.getPageNumber(),
                        DataPage.getFso(page),
                        recordBytes
                );
                bufferPool.markDirtyPage(
                        page.getPageNumber(),
                        logRecord.getStartLsn()
                );
                long endLsn = logRecord.getEndLsn();
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
        byte[] beforeImage = record.getBeforeImage();
        ByteSlice recordView = record.recordView();
        byte[] afterImage = Arrays.copyOfRange(
                recordView.bytes(),
                recordView.offset(),
                recordView.end()
        );
        long uid = record.getUid();

        LogRecord logRecord = transactionLogManager.appendUpdate(
                xid,
                UidUtil.getPgno(uid),
                UidUtil.getOffset(uid),
                beforeImage,
                afterImage
        );
        bufferPool.markDirtyPage(
                record.getPage().getPageNumber(),
                logRecord.getStartLsn()
        );
        long endLsn = logRecord.getEndLsn();

        Page page = record.getPage();
        page.wLock();
        try {
            page.setPageLsn(endLsn);
        } finally {
            page.wUnlock();
        }
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        // 注入的 BufferPool 与 WAL 由 DatabaseContext 管理生命周期。
        MetaPage.setVcClose(metaPage);
        bufferPool.persistMetaPage(metaPage);
        metaPage.release();
    }

    private void initializeMetaPage() {
        int pageNumber = bufferPool.newPage(MetaPage.newPageBytes());
        assert pageNumber == 1;
        try {
            metaPage = bufferPool.getPage(pageNumber);
        } catch (Exception e) {
            Panic.of(e);
        }
        bufferPool.persistMetaPage(metaPage);
    }

    private boolean loadAndCheckMetaPage() {
        try {
            metaPage = bufferPool.getPage(1);
        } catch (Exception e) {
            Panic.of(e);
        }
        MetaPage.requireSupportedFormat(metaPage);
        return MetaPage.checkVc(metaPage);
    }

    private void initializeFreeSpaceMap() {
        Map<Integer, Integer> freeSpaceByPage = bufferPool.getPageFreeMap();
        for (Map.Entry<Integer, Integer> entry : freeSpaceByPage.entrySet()) {
            freeSpaceMap.add(entry.getKey(), entry.getValue());
        }
    }
}
