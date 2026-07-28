package com.minisql.engine.storage.wal;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.engine.storage.page.PageBufferPool;
import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.transaction.xid.XidStatusTable;

/**
 * 管理事务维度的 WAL 语义：事务日志链、运行期 ATT 与事务状态日志。
 *
 * <p>{@link WriteAheadLogManager} 只负责物理 WAL 的追加、读取与持久化；
 * 本类负责解释 xid、prevLsn、lastLsn 以及 BEGIN/COMMIT/ABORT/END。</p>
 */
public final class TransactionLogManager {

    private final WriteAheadLogManager writeAheadLogManager;
    private final ActiveTransactionTable activeTransactionTable;
    private final XidStatusTable xidStatusTable;
    private final PageBufferPool bufferPool;
    private final Lock logChainLock = new ReentrantLock();

    public TransactionLogManager(
            WriteAheadLogManager writeAheadLogManager,
            ActiveTransactionTable activeTransactionTable,
            XidStatusTable xidStatusTable,
            PageBufferPool bufferPool
    ) {
        this.writeAheadLogManager = Objects.requireNonNull(
                writeAheadLogManager,
                "writeAheadLogManager must not be null"
        );
        this.activeTransactionTable = Objects.requireNonNull(
                activeTransactionTable,
                "activeTransactionTable must not be null"
        );
        this.xidStatusTable = Objects.requireNonNull(
                xidStatusTable,
                "xidStatusTable must not be null"
        );
        this.bufferPool = Objects.requireNonNull(
                bufferPool,
                "bufferPool must not be null"
        );
    }

    public void begin(long xid) {
        if (xid <= XidAllocator.SYSTEM_XID) {
            throw new IllegalArgumentException("xid must be positive: " + xid);
        }

        logChainLock.lock();
        try {
            activeTransactionTable.add(
                    xid,
                    ActiveTransaction.Status.ACTIVE,
                    LogRecord.NO_LSN
            );
            LogRecord beginRecord = appendTransactionState(
                    LogRecordType.BEGIN,
                    xid,
                    LogRecord.NO_LSN
            );
            updateLastLsn(xid, beginRecord.getStartLsn());
            writeAheadLogManager.flush(beginRecord.getEndLsn());
        } catch (RuntimeException exception) {
            activeTransactionTable.remove(xid);
            throw exception;
        } finally {
            logChainLock.unlock();
        }
    }

    public LogRecord appendInsert(long xid, int pageNumber, short recordOffset, byte[] recordBytes) {
        logChainLock.lock();
        try {
            LogRecord record = writeAheadLogManager.append(
                    LogRecordCodec.encodeInsert(
                            xid,
                            getLastLsn(xid),
                            pageNumber,
                            recordOffset,
                            recordBytes
                    )
            );
            updateLastLsn(xid, record.getStartLsn());
            return record;
        } finally {
            logChainLock.unlock();
        }
    }

    public LogRecord appendUpdate(long xid, int pageNumber, short recordOffset, byte[] beforeImage, byte[] afterImage) {
        logChainLock.lock();
        try {
            LogRecord record = writeAheadLogManager.append(
                    LogRecordCodec.encodeUpdate(
                            xid,
                            getLastLsn(xid),
                            pageNumber,
                            recordOffset,
                            beforeImage,
                            afterImage
                    )
            );
            updateLastLsn(xid, record.getStartLsn());
            return record;
        } finally {
            logChainLock.unlock();
        }
    }

    public LogRecord appendCommit(long xid) {
        return appendFinalState(LogRecordType.COMMIT, xid);
    }

    public LogRecord appendAbort(long xid) {
        return appendFinalState(LogRecordType.ABORT, xid);
    }

    /**
     * 追加 END 并从 ATT 删除事务。两个动作与 checkpoint snapshot
     * 使用同一把锁，避免快照观察到“已有 END、ATT 仍未删除”的中间状态。
     */
    public void complete(long xid) {
        logChainLock.lock();
        try {
            appendTransactionState(
                    LogRecordType.END,
                    xid,
                    getLastLsn(xid)
            );
            activeTransactionTable.remove(xid);
        } finally {
            logChainLock.unlock();
        }
    }

    public void undo(long xid, LogRecord abortRecord) {
        flush(abortRecord.getEndLsn());
        Recovery.undoTransaction(
                xidStatusTable,
                writeAheadLogManager,
                bufferPool,
                xid,
                abortRecord.getStartLsn()
        );
        logChainLock.lock();
        try {
            activeTransactionTable.remove(xid);
        } finally {
            logChainLock.unlock();
        }
    }

    public void flush(long endLsn) {
        if (endLsn > LogRecord.NO_LSN) {
            writeAheadLogManager.flush(endLsn);
        }
    }

    /**
     * 在事务日志链不发生变化的同一时间点捕获 DPT 与 ATT。
     * 包内可见，专供 CheckpointManager 使用。
     */
    CheckpointSnapshot captureCheckpointSnapshot() {
        logChainLock.lock();
        try {
            return new CheckpointSnapshot(
                    bufferPool.snapshotDirtyPageTable(),
                    activeTransactionTable.snapshot()
            );
        } finally {
            logChainLock.unlock();
        }
    }

    private LogRecord appendFinalState(LogRecordType type, long xid) {
        logChainLock.lock();
        try {
            LogRecord record = appendTransactionState(
                    type,
                    xid,
                    getLastLsn(xid)
            );
            ActiveTransaction.Status status =
                    type == LogRecordType.COMMIT
                            ? ActiveTransaction.Status.COMMITTING
                            : ActiveTransaction.Status.ABORTING;
            activeTransactionTable.update(
                    xid,
                    status,
                    record.getStartLsn()
            );
            return record;
        } finally {
            logChainLock.unlock();
        }
    }

    private LogRecord appendTransactionState(LogRecordType type, long xid, long prevLsn) {
        return writeAheadLogManager.append(
                LogRecordCodec.encodeTransactionState(type, xid, prevLsn)
        );
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

    static final class CheckpointSnapshot {
        private final Map<Integer, Long> dirtyPageTableEntries;
        private final Map<Long, ActiveTransaction> activeTransactions;

        private CheckpointSnapshot(
                Map<Integer, Long> dirtyPageTableEntries,
                Map<Long, ActiveTransaction> activeTransactions
        ) {
            this.dirtyPageTableEntries = dirtyPageTableEntries;
            this.activeTransactions = activeTransactions;
        }

        Map<Integer, Long> getDirtyPageTableEntries() {
            return dirtyPageTableEntries;
        }

        Map<Long, ActiveTransaction> getActiveTransactions() {
            return activeTransactions;
        }
    }
}
