package com.minisql.engine.storage.wal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.minisql.engine.storage.page.PageBufferPool;

/**
 * 创建不阻塞事务与 PageCleaner 的 fuzzy checkpoint。
 */
public final class CheckpointManager {

    private static final int MAX_ENTRIES_PER_CHUNK = 256;

    private final WriteAheadLogger walLogger;
    private final PageBufferPool bufferPool;
    private final ActiveTransactionTable activeTransactionTable;
    private final Lock snapshotLock;

    public CheckpointManager(
            WriteAheadLogger walLogger,
            PageBufferPool bufferPool,
            ActiveTransactionTable activeTransactionTable,
            Lock snapshotLock
    ) {
        this.walLogger = java.util.Objects.requireNonNull(
                walLogger,
                "walLogger must not be null"
        );
        this.bufferPool = java.util.Objects.requireNonNull(
                bufferPool,
                "bufferPool must not be null"
        );
        this.activeTransactionTable = java.util.Objects.requireNonNull(
                activeTransactionTable,
                "activeTransactionTable must not be null"
        );
        this.snapshotLock = java.util.Objects.requireNonNull(
                snapshotLock,
                "snapshotLock must not be null"
        );
    }

    public synchronized long checkpoint() {
        LogRecord beginRecord = walLogger.append(
                LogRecordCodec.encodeBeginCheckpoint()
        );
        long beginCheckpointLsn = beginRecord.getStartLsn();

        Map<Integer, Long> dirtyPages;
        Map<Long, ActiveTransaction> activeTransactions;
        snapshotLock.lock();
        try {
            dirtyPages = bufferPool.snapshotDirtyPages();
            activeTransactions = activeTransactionTable.snapshot();
        } finally {
            snapshotLock.unlock();
        }

        List<Map<Integer, Long>> dptChunks = partition(dirtyPages);
        for (int chunkIndex = 0; chunkIndex < dptChunks.size(); chunkIndex++) {
            walLogger.append(
                    LogRecordCodec.encodeCheckpointDpt(
                            beginCheckpointLsn,
                            chunkIndex,
                            dptChunks.get(chunkIndex)
                    )
            );
        }

        List<Map<Long, ActiveTransaction>> attChunks =
                partition(activeTransactions);
        for (int chunkIndex = 0; chunkIndex < attChunks.size(); chunkIndex++) {
            walLogger.append(
                    LogRecordCodec.encodeCheckpointAtt(
                            beginCheckpointLsn,
                            chunkIndex,
                            attChunks.get(chunkIndex)
                    )
            );
        }

        LogRecord endRecord = walLogger.append(
                LogRecordCodec.encodeEndCheckpoint(
                        beginCheckpointLsn,
                        dptChunks.size(),
                        attChunks.size()
                )
        );
        walLogger.flush(endRecord.getEndLsn());
        walLogger.setCheckpointLsn(beginCheckpointLsn);
        return beginCheckpointLsn;
    }

    private static <K, V> List<Map<K, V>> partition(Map<K, V> entries) {
        List<Map<K, V>> chunks = new ArrayList<>();
        Map<K, V> chunk = null;
        for (Map.Entry<K, V> entry : entries.entrySet()) {
            if (chunk == null || chunk.size() == MAX_ENTRIES_PER_CHUNK) {
                chunk = new LinkedHashMap<>();
                chunks.add(chunk);
            }
            chunk.put(entry.getKey(), entry.getValue());
        }
        return chunks;
    }
}
