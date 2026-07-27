package com.minisql.engine.storage.wal;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import com.minisql.engine.storage.page.DataPage;
import com.minisql.engine.storage.page.DirtyPageTable;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.storage.record.PageRecord;
import com.minisql.engine.transaction.status.TransactionManager;
import com.minisql.error.Panic;

/**
 * 基于 WAL 执行 Analysis、repeat-history Redo 与 restartable Undo。
 */
public final class Recovery {

    private Recovery() {
    }

    public static void recover(
            TransactionManager transactionManager,
            LogManager logManager,
            PageCache pageCache
    ) {
        System.out.println("Recovering...");

        AnalysisResult analysis = analyze(transactionManager, logManager);

        redo(logManager, pageCache, analysis.dirtyPageTable);
        System.out.println("Redo Transactions Over.");

        undo(
                transactionManager,
                logManager,
                pageCache,
                analysis.activeTransactionTable
        );
        System.out.println("Undo Transactions Over.");
        System.out.println("Recovery Over.");
    }

    public static void undoTransaction(
            TransactionManager transactionManager,
            LogManager logManager,
            PageCache pageCache,
            long xid,
            long lastLsn
    ) {
        ActiveTransactionTable activeTransactionTable =
                new ActiveTransactionTable();
        activeTransactionTable.add(
                xid,
                ActiveTransaction.Status.ABORTING,
                lastLsn
        );
        undo(
                transactionManager,
                logManager,
                pageCache,
                activeTransactionTable
        );
    }

    private static AnalysisResult analyze(
            TransactionManager transactionManager,
            LogManager logManager
    ) {
        DirtyPageTable dirtyPageTable = new DirtyPageTable();
        ActiveTransactionTable activeTransactionTable =
                new ActiveTransactionTable();

        try (LogManager.LogReader reader = logManager.getReader()) {
            reader.seek(logManager.getCheckpointLsn());
            CheckpointAccumulator checkpoint = null;
            while (true) {
                LogRecord record = reader.next();
                if (record == null) {
                    break;
                }
                switch (record.getType()) {
                    case BEGIN_CHECKPOINT:
                        LogRecordCodec.decodeBeginCheckpoint(record.getPayload());
                        checkpoint = new CheckpointAccumulator(record.getStartLsn());
                        continue;
                    case CHECKPOINT_DPT:
                        requireCheckpoint(checkpoint, record)
                                .add(LogRecordCodec.decodeCheckpointDpt(record.getPayload()));
                        continue;
                    case CHECKPOINT_ATT:
                        requireCheckpoint(checkpoint, record)
                                .add(LogRecordCodec.decodeCheckpointAtt(record.getPayload()));
                        continue;
                    case END_CHECKPOINT:
                        CheckpointAccumulator completeCheckpoint =
                                requireCheckpoint(checkpoint, record);
                        completeCheckpoint.complete(
                                LogRecordCodec.decodeEndCheckpoint(record.getPayload()),
                                dirtyPageTable,
                                activeTransactionTable
                        );
                        checkpoint = null;
                        continue;
                    default:
                        break;
                }
                analyzeRecord(
                        transactionManager,
                        dirtyPageTable,
                        activeTransactionTable,
                        record
                );
            }
        }

        for (ActiveTransaction transaction
                : activeTransactionTable.snapshot().values()) {
            if (transaction.getStatus()
                    != ActiveTransaction.Status.COMMITTING) {
                continue;
            }
            transactionManager.commit(transaction.getXid());
            LogRecord endRecord = logManager.append(
                    LogRecordCodec.encodeTransactionState(
                            LogRecordType.END,
                            transaction.getXid(),
                            transaction.getLastLsn()
                    )
            );
            logManager.flush(endRecord.getEndLsn());
            transactionManager.complete(transaction.getXid());
            activeTransactionTable.remove(transaction.getXid());
        }

        return new AnalysisResult(dirtyPageTable, activeTransactionTable);
    }

    private static CheckpointAccumulator requireCheckpoint(
            CheckpointAccumulator checkpoint,
            LogRecord record
    ) {
        if (checkpoint == null) {
            throw new IllegalStateException(
                    "Checkpoint record " + record.getType()
                            + " has no preceding BEGIN_CHECKPOINT at LSN "
                            + record.getStartLsn()
            );
        }
        return checkpoint;
    }

    private static void analyzeRecord(
            TransactionManager transactionManager,
            DirtyPageTable dirtyPageTable,
            ActiveTransactionTable activeTransactionTable,
            LogRecord record
    ) {
        LogRecordType type = record.getType();
        switch (type) {
            case BEGIN: {
                LogRecordCodec.TransactionStatePayload payload =
                        LogRecordCodec.decodeTransactionState(record.getPayload());
                activeTransactionTable.add(
                        payload.getXid(),
                        ActiveTransaction.Status.ACTIVE,
                        record.getStartLsn()
                );
                break;
            }
            case INSERT: {
                LogRecordCodec.InsertPayload payload =
                        LogRecordCodec.decodeInsert(record.getPayload());
                dirtyPageTable.mark(payload.getPageNumber(), record.getStartLsn());
                updateLastLsn(
                        activeTransactionTable,
                        payload.getXid(),
                        record.getStartLsn(),
                        ActiveTransaction.Status.ACTIVE
                );
                break;
            }
            case UPDATE: {
                LogRecordCodec.UpdatePayload payload =
                        LogRecordCodec.decodeUpdate(record.getPayload());
                dirtyPageTable.mark(payload.getPageNumber(), record.getStartLsn());
                updateLastLsn(
                        activeTransactionTable,
                        payload.getXid(),
                        record.getStartLsn(),
                        ActiveTransaction.Status.ACTIVE
                );
                break;
            }
            case CLR: {
                LogRecordCodec.ClrPayload payload =
                        LogRecordCodec.decodeClr(record.getPayload());
                dirtyPageTable.mark(payload.getPageNumber(), record.getStartLsn());
                updateLastLsn(
                        activeTransactionTable,
                        payload.getXid(),
                        record.getStartLsn(),
                        ActiveTransaction.Status.ABORTING
                );
                break;
            }
            case COMMIT: {
                LogRecordCodec.TransactionStatePayload payload =
                        LogRecordCodec.decodeTransactionState(record.getPayload());
                activeTransactionTable.update(
                        payload.getXid(),
                        ActiveTransaction.Status.COMMITTING,
                        record.getStartLsn()
                );
                if (!transactionManager.isCommitted(payload.getXid())) {
                    transactionManager.commit(payload.getXid());
                }
                break;
            }
            case ABORT: {
                LogRecordCodec.TransactionStatePayload payload =
                        LogRecordCodec.decodeTransactionState(record.getPayload());
                activeTransactionTable.update(
                        payload.getXid(),
                        ActiveTransaction.Status.ABORTING,
                        record.getStartLsn()
                );
                break;
            }
            case END: {
                LogRecordCodec.TransactionStatePayload payload =
                        LogRecordCodec.decodeTransactionState(record.getPayload());
                activeTransactionTable.remove(payload.getXid());
                transactionManager.complete(payload.getXid());
                break;
            }
            case BEGIN_CHECKPOINT:
            case CHECKPOINT_DPT:
            case CHECKPOINT_ATT:
            case END_CHECKPOINT:
                throw new IllegalStateException(
                        "Checkpoint record must be handled by checkpoint accumulator"
                );
            default:
                throw new IllegalStateException("Unhandled WAL record type: " + type);
        }
    }

    private static void updateLastLsn(
            ActiveTransactionTable activeTransactionTable,
            long xid,
            long lastLsn,
            ActiveTransaction.Status defaultStatus
    ) {
        if (xid <= 0) {
            return;
        }
        ActiveTransaction transaction =
                activeTransactionTable.get(xid);
        ActiveTransaction.Status status = transaction == null
                ? defaultStatus
                : transaction.getStatus();
        activeTransactionTable.update(xid, status, lastLsn);
    }

    private static void redo(
            LogManager logManager,
            PageCache pageCache,
            DirtyPageTable dirtyPageTable
    ) {
        long redoLsn = dirtyPageTable.getMinRecoveryLsn();
        if (redoLsn == Long.MAX_VALUE) {
            return;
        }

        try (LogManager.LogReader reader = logManager.getReader()) {
            reader.seek(redoLsn);
            while (true) {
                LogRecord record = reader.next();
                if (record == null) {
                    return;
                }
                if (!record.getType().isRedoablePageChange()) {
                    continue;
                }

                int pageNumber = getPageNumber(record);
                Long recoveryLsn = dirtyPageTable.getRecoveryLsn(pageNumber);
                if (recoveryLsn == null || record.getStartLsn() < recoveryLsn) {
                    continue;
                }

                switch (record.getType()) {
                    case INSERT:
                        replayInsert(pageCache, record);
                        break;
                    case UPDATE:
                        replayUpdate(pageCache, record);
                        break;
                    case CLR:
                        replayClr(pageCache, record);
                        break;
                    default:
                        throw new IllegalStateException(
                                "Unhandled redo record type: " + record.getType()
                        );
                }
            }
        }
    }

    private static void undo(
            TransactionManager transactionManager,
            LogManager logManager,
            PageCache pageCache,
            ActiveTransactionTable activeTransactionTable
    ) {
        PriorityQueue<UndoTask> tasks = new PriorityQueue<>(
                Comparator.comparingLong(UndoTask::getNextLsn).reversed()
        );

        for (ActiveTransaction transaction
                : activeTransactionTable.snapshot().values()) {
            if (transaction.getStatus() == ActiveTransaction.Status.COMMITTING) {
                continue;
            }
            if (transaction.getStatus() == ActiveTransaction.Status.ACTIVE) {
                LogRecord abortRecord = logManager.append(
                        LogRecordCodec.encodeTransactionState(
                                LogRecordType.ABORT,
                                transaction.getXid(),
                                transaction.getLastLsn()
                        )
                );
                activeTransactionTable.update(
                        transaction.getXid(),
                        ActiveTransaction.Status.ABORTING,
                        abortRecord.getStartLsn()
                );
                tasks.add(new UndoTask(
                        transaction.getXid(),
                        transaction.getLastLsn()
                ));
            } else {
                tasks.add(new UndoTask(
                        transaction.getXid(),
                        transaction.getLastLsn()
                ));
            }
        }

        try (LogManager.LogReader reader = logManager.getReader()) {
            while (!tasks.isEmpty()) {
                UndoTask task = tasks.remove();
                LogRecord record = readRecord(reader, task.nextLsn);
                long nextLsn = undoRecord(
                        logManager,
                        pageCache,
                        activeTransactionTable,
                        task.xid,
                        record
                );
                if (nextLsn == LogRecord.NO_LSN) {
                    finishUndo(
                            transactionManager,
                            logManager,
                            activeTransactionTable,
                            task.xid
                    );
                } else {
                    tasks.add(new UndoTask(task.xid, nextLsn));
                }
            }
        }
    }

    private static long undoRecord(
            LogManager logManager,
            PageCache pageCache,
            ActiveTransactionTable activeTransactionTable,
            long xid,
            LogRecord record
    ) {
        switch (record.getType()) {
            case BEGIN:
                return LogRecord.NO_LSN;
            case ABORT:
                return requireTransactionState(record, xid).getPrevLsn();
            case CLR:
                return requireClr(record, xid).getUndoNextLsn();
            case INSERT: {
                LogRecordCodec.InsertPayload payload =
                        LogRecordCodec.decodeInsert(record.getPayload());
                requireXid(xid, payload.getXid(), record);
                byte[] compensationImage = payload.getRecordBytes();
                PageRecord.markInvalid(compensationImage);
                return appendClrAndApply(
                        logManager,
                        pageCache,
                        activeTransactionTable,
                        xid,
                        payload.getPrevLsn(),
                        payload.getPageNumber(),
                        payload.getRecordOffsetInPage(),
                        compensationImage
                );
            }
            case UPDATE: {
                LogRecordCodec.UpdatePayload payload =
                        LogRecordCodec.decodeUpdate(record.getPayload());
                requireXid(xid, payload.getXid(), record);
                return appendClrAndApply(
                        logManager,
                        pageCache,
                        activeTransactionTable,
                        xid,
                        payload.getPrevLsn(),
                        payload.getPageNumber(),
                        payload.getRecordOffsetInPage(),
                        payload.getBeforeImage()
                );
            }
            default:
                throw new IllegalStateException(
                        "Record type cannot be undone: " + record.getType()
                );
        }
    }

    private static long appendClrAndApply(
            LogManager logManager,
            PageCache pageCache,
            ActiveTransactionTable activeTransactionTable,
            long xid,
            long undoNextLsn,
            int pageNumber,
            short recordOffsetInPage,
            byte[] compensationImage
    ) {
        ActiveTransaction transaction =
                activeTransactionTable.get(xid);
        if (transaction == null) {
            throw new IllegalStateException("Missing active transaction: " + xid);
        }
        byte[] clrPayload = LogRecordCodec.encodeClr(
                xid,
                transaction.getLastLsn(),
                undoNextLsn,
                pageNumber,
                recordOffsetInPage,
                compensationImage
        );
        LogRecord clrRecord = logManager.append(clrPayload);
        activeTransactionTable.update(
                xid,
                ActiveTransaction.Status.ABORTING,
                clrRecord.getStartLsn()
        );
        applyPageImage(
                pageCache,
                clrRecord,
                pageNumber,
                recordOffsetInPage,
                compensationImage
        );
        return undoNextLsn;
    }

    private static void finishUndo(
            TransactionManager transactionManager,
            LogManager logManager,
            ActiveTransactionTable activeTransactionTable,
            long xid
    ) {
        ActiveTransaction transaction =
                activeTransactionTable.get(xid);
        if (transaction == null) {
            return;
        }
        LogRecord endRecord = logManager.append(
                LogRecordCodec.encodeTransactionState(
                        LogRecordType.END,
                        xid,
                        transaction.getLastLsn()
                )
        );
        logManager.flush(endRecord.getEndLsn());
        transactionManager.abort(xid);
        transactionManager.complete(xid);
        activeTransactionTable.remove(xid);
    }

    private static LogRecord readRecord(LogManager.LogReader reader, long startLsn) {
        reader.seek(startLsn);
        LogRecord record = reader.next();
        if (record == null || record.getStartLsn() != startLsn) {
            throw new IllegalStateException("Missing WAL record at LSN " + startLsn);
        }
        return record;
    }

    private static LogRecordCodec.TransactionStatePayload requireTransactionState(
            LogRecord record,
            long expectedXid
    ) {
        LogRecordCodec.TransactionStatePayload payload =
                LogRecordCodec.decodeTransactionState(record.getPayload());
        requireXid(expectedXid, payload.getXid(), record);
        return payload;
    }

    private static LogRecordCodec.ClrPayload requireClr(
            LogRecord record,
            long expectedXid
    ) {
        LogRecordCodec.ClrPayload payload =
                LogRecordCodec.decodeClr(record.getPayload());
        requireXid(expectedXid, payload.getXid(), record);
        return payload;
    }

    private static void requireXid(
            long expectedXid,
            long actualXid,
            LogRecord record
    ) {
        if (actualXid != expectedXid) {
            throw new IllegalStateException(
                    "WAL chain for XID " + expectedXid
                            + " points to XID " + actualXid
                            + " at LSN " + record.getStartLsn()
            );
        }
    }

    private static int getPageNumber(LogRecord record) {
        switch (record.getType()) {
            case INSERT:
                return LogRecordCodec.decodeInsert(record.getPayload()).getPageNumber();
            case UPDATE:
                return LogRecordCodec.decodeUpdate(record.getPayload()).getPageNumber();
            case CLR:
                return LogRecordCodec.decodeClr(record.getPayload()).getPageNumber();
            default:
                throw new IllegalArgumentException(
                        "Record does not modify a Page: " + record.getType()
                );
        }
    }

    private static void replayInsert(PageCache pageCache, LogRecord record) {
        LogRecordCodec.InsertPayload payload =
                LogRecordCodec.decodeInsert(record.getPayload());
        applyPageImage(
                pageCache,
                record,
                payload.getPageNumber(),
                payload.getRecordOffsetInPage(),
                payload.getRecordBytes()
        );
    }

    private static void replayUpdate(PageCache pageCache, LogRecord record) {
        LogRecordCodec.UpdatePayload payload =
                LogRecordCodec.decodeUpdate(record.getPayload());
        applyPageImage(
                pageCache,
                record,
                payload.getPageNumber(),
                payload.getRecordOffsetInPage(),
                payload.getAfterImage()
        );
    }

    private static void replayClr(PageCache pageCache, LogRecord record) {
        LogRecordCodec.ClrPayload payload =
                LogRecordCodec.decodeClr(record.getPayload());
        applyPageImage(
                pageCache,
                record,
                payload.getPageNumber(),
                payload.getRecordOffsetInPage(),
                payload.getCompensationImage()
        );
    }

    private static void applyPageImage(
            PageCache pageCache,
            LogRecord record,
            int pageNumber,
            short recordOffsetInPage,
            byte[] recordBytes
    ) {
        Page page;
        try {
            page = pageCache.getPage(pageNumber);
        } catch (Exception exception) {
            Panic.of(exception);
            return;
        }
        try {
            page.wLock();
            try {
                if (page.getPageLsn() >= record.getEndLsn()) {
                    return;
                }
                DataPage.recoverInsert(page, recordBytes, recordOffsetInPage);
                page.setPageLsn(record.getEndLsn());
                pageCache.markDirtyPage(pageNumber, record.getStartLsn());
            } finally {
                page.wUnlock();
            }
        } finally {
            page.release();
        }
    }

    private static final class AnalysisResult {
        private final DirtyPageTable dirtyPageTable;
        private final ActiveTransactionTable activeTransactionTable;

        private AnalysisResult(
                DirtyPageTable dirtyPageTable,
                ActiveTransactionTable activeTransactionTable
        ) {
            this.dirtyPageTable = dirtyPageTable;
            this.activeTransactionTable = activeTransactionTable;
        }
    }

    private static final class UndoTask {
        private final long xid;
        private final long nextLsn;

        private UndoTask(long xid, long nextLsn) {
            this.xid = xid;
            this.nextLsn = nextLsn;
        }

        private long getNextLsn() {
            return nextLsn;
        }
    }

    private static final class CheckpointAccumulator {
        private final long beginCheckpointLsn;
        private final Map<Integer, LogRecordCodec.CheckpointDptPayload> dptChunks =
                new HashMap<>();
        private final Map<Integer, LogRecordCodec.CheckpointAttPayload> attChunks =
                new HashMap<>();

        private CheckpointAccumulator(long beginCheckpointLsn) {
            this.beginCheckpointLsn = beginCheckpointLsn;
        }

        private void add(LogRecordCodec.CheckpointDptPayload chunk) {
            requireOwner(chunk.getBeginCheckpointLsn());
            if (dptChunks.put(chunk.getChunkIndex(), chunk) != null) {
                throw new IllegalStateException(
                        "Duplicate DPT checkpoint chunk index: "
                                + chunk.getChunkIndex()
                );
            }
        }

        private void add(LogRecordCodec.CheckpointAttPayload chunk) {
            requireOwner(chunk.getBeginCheckpointLsn());
            if (attChunks.put(chunk.getChunkIndex(), chunk) != null) {
                throw new IllegalStateException(
                        "Duplicate ATT checkpoint chunk index: "
                                + chunk.getChunkIndex()
                );
            }
        }

        private void complete(
                LogRecordCodec.EndCheckpointPayload end,
                DirtyPageTable dirtyPageTable,
                ActiveTransactionTable activeTransactionTable
        ) {
            requireOwner(end.getBeginCheckpointLsn());
            requireCompleteChunkRange(dptChunks, end.getDptChunkCount(), "DPT");
            requireCompleteChunkRange(attChunks, end.getAttChunkCount(), "ATT");

            for (int index = 0; index < end.getDptChunkCount(); index++) {
                for (Map.Entry<Integer, Long> entry
                        : dptChunks.get(index).getEntries().entrySet()) {
                    dirtyPageTable.mark(entry.getKey(), entry.getValue());
                }
            }
            for (int index = 0; index < end.getAttChunkCount(); index++) {
                for (ActiveTransaction checkpointTransaction
                        : attChunks.get(index).getEntries().values()) {
                    ActiveTransaction analyzedTransaction =
                            activeTransactionTable.get(checkpointTransaction.getXid());
                    if (analyzedTransaction == null
                            || checkpointTransaction.getLastLsn()
                            > analyzedTransaction.getLastLsn()) {
                        activeTransactionTable.update(
                                checkpointTransaction.getXid(),
                                checkpointTransaction.getStatus(),
                                checkpointTransaction.getLastLsn()
                        );
                    }
                }
            }
        }

        private void requireOwner(long actualBeginCheckpointLsn) {
            if (actualBeginCheckpointLsn != beginCheckpointLsn) {
                throw new IllegalStateException(
                        "Checkpoint chunk belongs to BEGIN_CHECKPOINT "
                                + actualBeginCheckpointLsn
                                + " but expected " + beginCheckpointLsn
                );
            }
        }

        private static void requireCompleteChunkRange(
                Map<Integer, ?> chunks,
                int expectedCount,
                String name
        ) {
            if (chunks.size() != expectedCount) {
                throw new IllegalStateException(
                        name + " checkpoint chunk count mismatch: expected "
                                + expectedCount + " but found " + chunks.size()
                );
            }
            for (int index = 0; index < expectedCount; index++) {
                if (!chunks.containsKey(index)) {
                    throw new IllegalStateException(
                            "Missing " + name + " checkpoint chunk index " + index
                    );
                }
            }
        }
    }
}
