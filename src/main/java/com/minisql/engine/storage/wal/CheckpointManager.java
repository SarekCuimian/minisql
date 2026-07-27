package com.minisql.engine.storage.wal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.transaction.status.TransactionManager;

/**
 * 创建不阻塞事务与 PageCleaner 的 fuzzy checkpoint。
 */
public final class CheckpointManager {

    private static final int MAX_ENTRIES_PER_CHUNK = 256;

    private final LogManager logManager;
    private final PageCache pageCache;
    private final TransactionManager transactionManager;
    private final Lock snapshotLock;

    public CheckpointManager(
            LogManager logManager,
            PageCache pageCache,
            TransactionManager transactionManager,
            Lock snapshotLock
    ) {
        this.logManager = java.util.Objects.requireNonNull(
                logManager,
                "logManager must not be null"
        );
        this.pageCache = java.util.Objects.requireNonNull(
                pageCache,
                "pageCache must not be null"
        );
        this.transactionManager = java.util.Objects.requireNonNull(
                transactionManager,
                "transactionManager must not be null"
        );
        this.snapshotLock = java.util.Objects.requireNonNull(
                snapshotLock,
                "snapshotLock must not be null"
        );
    }

    public synchronized long checkpoint() {
        LogRecord beginRecord = logManager.append(
                LogRecordCodec.encodeBeginCheckpoint()
        );
        long beginCheckpointLsn = beginRecord.getStartLsn();

        Map<Integer, Long> dirtyPages;
        Map<Long, ActiveTransaction> activeTransactions;
        snapshotLock.lock();
        try {
            dirtyPages = pageCache.snapshotDirtyPages();
            activeTransactions = transactionManager.snapshotActiveTransactions();
        } finally {
            snapshotLock.unlock();
        }

        List<Map<Integer, Long>> dptChunks = partition(dirtyPages);
        for (int chunkIndex = 0; chunkIndex < dptChunks.size(); chunkIndex++) {
            logManager.append(
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
            logManager.append(
                    LogRecordCodec.encodeCheckpointAtt(
                            beginCheckpointLsn,
                            chunkIndex,
                            attChunks.get(chunkIndex)
                    )
            );
        }

        LogRecord endRecord = logManager.append(
                LogRecordCodec.encodeEndCheckpoint(
                        beginCheckpointLsn,
                        dptChunks.size(),
                        attChunks.size()
                )
        );
        logManager.flush(endRecord.getEndLsn());
        logManager.setCheckpointLsn(beginCheckpointLsn);
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
