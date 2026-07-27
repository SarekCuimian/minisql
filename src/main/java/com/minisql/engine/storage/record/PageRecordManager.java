package com.minisql.engine.storage.record;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.codec.UidUtil;
import com.minisql.engine.storage.page.DataPage;
import com.minisql.engine.storage.page.MetaPage;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.storage.page.fsm.FreeSpace;
import com.minisql.engine.storage.page.fsm.FreeSpaceMap;
import com.minisql.engine.storage.wal.LogManager;
import com.minisql.engine.storage.wal.CheckpointManager;
import com.minisql.engine.storage.wal.LogRecord;
import com.minisql.engine.storage.wal.LogRecordCodec;
import com.minisql.engine.storage.wal.LogRecordType;
import com.minisql.engine.storage.wal.Recovery;
import com.minisql.engine.transaction.status.TransactionManager;
import com.minisql.error.Error;
import com.minisql.error.Panic;

/** 协调页内 record 的存取、WAL 与空闲空间管理。 */
public final class PageRecordManager implements AutoCloseable {

    private final TransactionManager transactionManager;
    private final PageCache pageCache;
    private final LogManager logManager;
    private final FreeSpaceMap freeSpaceMap;
    private final Lock logChainLock;
    private Page metaPage;

    private PageRecordManager(PageCache pageCache, LogManager logManager, TransactionManager transactionManager) {
        this.pageCache = pageCache;
        this.logManager = logManager;
        this.transactionManager = transactionManager;
        this.freeSpaceMap = new FreeSpaceMap();
        this.logChainLock = new ReentrantLock();
        this.pageCache.setCheckpointManager(
                new CheckpointManager(
                        logManager,
                        pageCache,
                        transactionManager,
                        logChainLock
                )
        );
    }

    public static PageRecordManager create(String path, long memory, TransactionManager transactionManager) {
        LogManager logManager = LogManager.create(path);
        PageCache pageCache = PageCache.create(path, memory);

        PageRecordManager pageRecordManager = new PageRecordManager(pageCache, logManager, transactionManager);
        pageCache.setLogManager(logManager);
        pageRecordManager.initializeMetaPage();
        return pageRecordManager;
    }

    public static PageRecordManager open(String path, long memory, TransactionManager transactionManager) {
        LogManager logManager = LogManager.open(path);
        PageCache pageCache = PageCache.open(path, memory);

        PageRecordManager pageRecordManager = new PageRecordManager(pageCache, logManager, transactionManager);
        if (!pageRecordManager.loadAndCheckMetaPage()) {
            Recovery.recover(transactionManager, logManager, pageCache);
        }
        pageCache.setLogManager(logManager);
        pageRecordManager.initializeFreeSpaceMap();
        MetaPage.setVcOpen(pageRecordManager.metaPage);
        pageCache.persistMetaPage(pageRecordManager.metaPage);
        return pageRecordManager;
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
                LogRecord logRecord;
                logChainLock.lock();
                try {
                    long prevLsn = transactionManager.getLastLsn(xid);
                    byte[] logPayload = LogRecordCodec.encodeInsert(
                            xid,
                            prevLsn,
                            page.getPageNumber(),
                            DataPage.getFso(page),
                            recordBytes
                    );
                    logRecord = logManager.append(logPayload);
                    transactionManager.updateLastLsn(xid, logRecord.getStartLsn());
                    pageCache.markDirtyPage(
                            page.getPageNumber(),
                            logRecord.getStartLsn()
                    );
                } finally {
                    logChainLock.unlock();
                }
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

        LogRecord logRecord;
        logChainLock.lock();
        try {
            long prevLsn = transactionManager.getLastLsn(xid);
            byte[] logPayload = LogRecordCodec.encodeUpdate(
                    xid,
                    prevLsn,
                    UidUtil.getPgno(uid),
                    UidUtil.getOffset(uid),
                    beforeImage,
                    afterImage
            );
            logRecord = logManager.append(logPayload);
            transactionManager.updateLastLsn(xid, logRecord.getStartLsn());
            pageCache.markDirtyPage(
                    record.getPage().getPageNumber(),
                    logRecord.getStartLsn()
            );
        } finally {
            logChainLock.unlock();
        }
        long endLsn = logRecord.getEndLsn();

        Page page = record.getPage();
        page.wLock();
        try {
            page.setPageLsn(endLsn);
        } finally {
            page.wUnlock();
        }
    }

    public long beginTransaction() {
        logChainLock.lock();
        long xid = transactionManager.begin();
        try {
            byte[] payload = LogRecordCodec.encodeTransactionState(
                    LogRecordType.BEGIN,
                    xid,
                    LogRecord.NO_LSN
            );
            LogRecord beginRecord = logManager.append(payload);
            transactionManager.updateLastLsn(xid, beginRecord.getStartLsn());
            logManager.flush(beginRecord.getEndLsn());
            return xid;
        } catch (RuntimeException exception) {
            transactionManager.abort(xid);
            transactionManager.complete(xid);
            throw exception;
        } finally {
            logChainLock.unlock();
        }
    }

    public LogRecord appendCommitLog(long xid) {
        return appendLinkedTransactionStateLog(LogRecordType.COMMIT, xid);
    }

    public LogRecord appendAbortLog(long xid) {
        return appendLinkedTransactionStateLog(LogRecordType.ABORT, xid);
    }

    public LogRecord appendEndLog(long xid, long prevLsn) {
        logChainLock.lock();
        try {
            byte[] payload = LogRecordCodec.encodeTransactionState(
                    LogRecordType.END,
                    xid,
                    prevLsn
            );
            return logManager.append(payload);
        } finally {
            logChainLock.unlock();
        }
    }

    public void undoTransaction(long xid, LogRecord abortRecord) {
        logManager.flush(abortRecord.getEndLsn());
        Recovery.undoTransaction(
                transactionManager,
                logManager,
                pageCache,
                xid,
                abortRecord.getStartLsn()
        );
    }

    public void flushLog(long lsn) {
        if (lsn > 0) {
            logManager.flush(lsn);
        }
    }

    private LogRecord appendLinkedTransactionStateLog(LogRecordType type, long xid) {
        logChainLock.lock();
        try {
            long prevLsn = type == LogRecordType.BEGIN
                    ? LogRecord.NO_LSN
                    : transactionManager.getLastLsn(xid);
            byte[] payload = LogRecordCodec.encodeTransactionState(type, xid, prevLsn);
            LogRecord record = logManager.append(payload);
            transactionManager.updateLastLsn(xid, record.getStartLsn());
            if (type == LogRecordType.COMMIT) {
                transactionManager.markCommitting(xid);
            } else if (type == LogRecordType.ABORT) {
                transactionManager.markAborting(xid);
            }
            return record;
        } finally {
            logChainLock.unlock();
        }
    }

    @Override
    public void close() {
        MetaPage.setVcClose(metaPage);
        pageCache.persistMetaPage(metaPage);
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
        pageCache.persistMetaPage(metaPage);
    }

    private boolean loadAndCheckMetaPage() {
        try {
            metaPage = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.of(e);
        }
        MetaPage.requireSupportedFormat(metaPage);
        return MetaPage.checkVc(metaPage);
    }

    private void initializeFreeSpaceMap() {
        Map<Integer, Integer> freeSpaceByPage = pageCache.getPageFreeMap();
        for (Map.Entry<Integer, Integer> entry : freeSpaceByPage.entrySet()) {
            freeSpaceMap.add(entry.getKey(), entry.getValue());
        }
    }
}
