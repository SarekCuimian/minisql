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
import com.minisql.engine.storage.page.PageBufferPool;
import com.minisql.engine.storage.page.fsm.FreeSpace;
import com.minisql.engine.storage.page.fsm.FreeSpaceMap;
import com.minisql.engine.storage.wal.WriteAheadLogger;
import com.minisql.engine.storage.wal.CheckpointManager;
import com.minisql.engine.storage.wal.LogRecord;
import com.minisql.engine.storage.wal.LogRecordCodec;
import com.minisql.engine.storage.wal.LogRecordType;
import com.minisql.engine.storage.wal.Recovery;
import com.minisql.engine.storage.wal.ActiveTransaction;
import com.minisql.engine.storage.wal.ActiveTransactionTable;
import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.transaction.xid.XidStatusTable;
import com.minisql.error.Error;
import com.minisql.error.Panic;

/** 协调页内 record 的存取、WAL 与空闲空间管理。 */
public final class PageRecordManager implements AutoCloseable {

    private final XidStatusTable xidStatusTable;
    private final ActiveTransactionTable activeTransactionTable;
    private final PageBufferPool bufferPool;
    private final WriteAheadLogger walLogger;
    private final FreeSpaceMap freeSpaceMap;
    private final Lock logChainLock;
    private Page metaPage;

    private PageRecordManager(
            PageBufferPool bufferPool,
            WriteAheadLogger walLogger,
            XidStatusTable xidStatusTable,
            ActiveTransactionTable activeTransactionTable
    ) {
        this.bufferPool = bufferPool;
        this.walLogger = walLogger;
        this.xidStatusTable = xidStatusTable;
        this.activeTransactionTable = activeTransactionTable;
        this.freeSpaceMap = new FreeSpaceMap();
        this.logChainLock = new ReentrantLock();
        this.bufferPool.setCheckpointManager(
                new CheckpointManager(
                        walLogger,
                        bufferPool,
                        activeTransactionTable,
                        logChainLock
                )
        );
    }

    public static PageRecordManager create(
            String path,
            long memory,
            XidStatusTable xidStatusTable,
            ActiveTransactionTable activeTransactionTable
    ) {
        WriteAheadLogger walLogger = WriteAheadLogger.create(path);
        PageBufferPool bufferPool = PageBufferPool.create(path, memory);

        PageRecordManager pageRecordManager = new PageRecordManager(
                bufferPool,
                walLogger,
                xidStatusTable,
                activeTransactionTable
        );
        bufferPool.setWalLogger(walLogger);
        pageRecordManager.initializeMetaPage();
        return pageRecordManager;
    }

    public static PageRecordManager open(
            String path,
            long memory,
            XidStatusTable xidStatusTable,
            ActiveTransactionTable activeTransactionTable
    ) {
        WriteAheadLogger walLogger = WriteAheadLogger.open(path);
        PageBufferPool bufferPool = PageBufferPool.open(path, memory);

        PageRecordManager pageRecordManager = new PageRecordManager(
                bufferPool,
                walLogger,
                xidStatusTable,
                activeTransactionTable
        );
        if (!pageRecordManager.loadAndCheckMetaPage()) {
            Recovery.recover(xidStatusTable, walLogger, bufferPool);
        }
        bufferPool.setWalLogger(walLogger);
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
                LogRecord logRecord;
                logChainLock.lock();
                try {
                    long prevLsn = getLastLsn(xid);
                    byte[] logPayload = LogRecordCodec.encodeInsert(
                            xid,
                            prevLsn,
                            page.getPageNumber(),
                            DataPage.getFso(page),
                            recordBytes
                    );
                    logRecord = walLogger.append(logPayload);
                    updateLastLsn(xid, logRecord.getStartLsn());
                    bufferPool.markDirtyPage(
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
            long prevLsn = getLastLsn(xid);
            byte[] logPayload = LogRecordCodec.encodeUpdate(
                    xid,
                    prevLsn,
                    UidUtil.getPgno(uid),
                    UidUtil.getOffset(uid),
                    beforeImage,
                    afterImage
            );
            logRecord = walLogger.append(logPayload);
            updateLastLsn(xid, logRecord.getStartLsn());
            bufferPool.markDirtyPage(
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

    public void beginTransaction(long xid) {
        if (xid <= XidAllocator.SYSTEM_XID) {
            throw new IllegalArgumentException("XID must be positive: " + xid);
        }
        logChainLock.lock();
        activeTransactionTable.add(
                xid,
                ActiveTransaction.Status.ACTIVE,
                LogRecord.NO_LSN
        );
        try {
            byte[] payload = LogRecordCodec.encodeTransactionState(
                    LogRecordType.BEGIN,
                    xid,
                    LogRecord.NO_LSN
            );
            LogRecord beginRecord = walLogger.append(payload);
            updateLastLsn(xid, beginRecord.getStartLsn());
            walLogger.flush(beginRecord.getEndLsn());
        } catch (RuntimeException exception) {
            xidStatusTable.recordAborted(xid);
            activeTransactionTable.remove(xid);
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
            return walLogger.append(payload);
        } finally {
            logChainLock.unlock();
        }
    }

    public void undoTransaction(long xid, LogRecord abortRecord) {
        walLogger.flush(abortRecord.getEndLsn());
        Recovery.undoTransaction(
                xidStatusTable,
                walLogger,
                bufferPool,
                xid,
                abortRecord.getStartLsn()
        );
        activeTransactionTable.remove(xid);
    }

    public void flushLog(long lsn) {
        if (lsn > 0) {
            walLogger.flush(lsn);
        }
    }

    private LogRecord appendLinkedTransactionStateLog(LogRecordType type, long xid) {
        logChainLock.lock();
        try {
            long prevLsn = type == LogRecordType.BEGIN
                    ? LogRecord.NO_LSN
                    : getLastLsn(xid);
            byte[] payload = LogRecordCodec.encodeTransactionState(type, xid, prevLsn);
            LogRecord record = walLogger.append(payload);
            updateLastLsn(xid, record.getStartLsn());
            if (type == LogRecordType.COMMIT) {
                activeTransactionTable.update(
                        xid,
                        ActiveTransaction.Status.COMMITTING,
                        record.getStartLsn()
                );
            } else if (type == LogRecordType.ABORT) {
                activeTransactionTable.update(
                        xid,
                        ActiveTransaction.Status.ABORTING,
                        record.getStartLsn()
                );
            }
            return record;
        } finally {
            logChainLock.unlock();
        }
    }

    public void completeTransaction(long xid) {
        activeTransactionTable.remove(xid);
    }

    private long getLastLsn(long xid) {
        if (xid <= XidAllocator.SYSTEM_XID) {
            return LogRecord.NO_LSN;
        }
        ActiveTransaction transaction = activeTransactionTable.get(xid);
        if (transaction == null) {
            throw new IllegalStateException(
                    "Missing active WAL transaction: " + xid
            );
        }
        return transaction.getLastLsn();
    }

    private void updateLastLsn(long xid, long lastLsn) {
        if (xid <= XidAllocator.SYSTEM_XID) {
            return;
        }
        ActiveTransaction transaction = activeTransactionTable.get(xid);
        ActiveTransaction.Status status = transaction == null
                ? ActiveTransaction.Status.ACTIVE
                : transaction.getStatus();
        activeTransactionTable.update(xid, status, lastLsn);
    }
    public void close() {
        MetaPage.setVcClose(metaPage);
        bufferPool.persistMetaPage(metaPage);
        metaPage.release();
        bufferPool.close();
        walLogger.close();
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
