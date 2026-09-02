package com.minisql.engine.storage.wal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.minisql.engine.storage.wal.TransactionLogManager.CheckpointSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建不阻塞事务与 PageCleaner 的 fuzzy checkpoint。
 */
public final class CheckpointManager {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CheckpointManager.class);
    private static final int MAX_ENTRIES_PER_CHUNK = 256;
    private static final long CHECKPOINT_INTERVAL_SECONDS = 30L;

    private final WriteAheadLogManager writeAheadLogManager;
    private final TransactionLogManager transactionLogManager;
    private final Object lifecycleLock = new Object();
    private ScheduledExecutorService scheduler;
    private boolean stopped;

    public CheckpointManager(WriteAheadLogManager writeAheadLogManager, TransactionLogManager transactionLogManager) {
        this.writeAheadLogManager = java.util.Objects.requireNonNull(
                writeAheadLogManager,
                "writeAheadLogManager must not be null"
        );
        this.transactionLogManager = java.util.Objects.requireNonNull(
                transactionLogManager,
                "transactionLogManager must not be null"
        );
    }

    /**
     * 组件组装与 recovery 完成后启动周期 checkpoint。
     */
    public void start() {
        synchronized (lifecycleLock) {
            if (stopped) {
                throw new IllegalStateException(
                        "Checkpoint manager is already stopped"
                );
            }
            if (scheduler != null) {
                return;
            }
            scheduler = Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(
                                runnable,
                                "CheckpointScheduler"
                        );
                        thread.setDaemon(true);
                        return thread;
                    }
            );
            scheduler.scheduleWithFixedDelay(
                    this::runScheduledCheckpoint,
                    CHECKPOINT_INTERVAL_SECONDS,
                    CHECKPOINT_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
            );
        }
    }

    /**
     * 停止调度并等待正在执行的 checkpoint 结束。
     */
    public void stop() {
        ScheduledExecutorService currentScheduler;
        synchronized (lifecycleLock) {
            if (stopped) {
                return;
            }
            stopped = true;
            currentScheduler = scheduler;
            scheduler = null;
        }
        if (currentScheduler == null) {
            return;
        }

        currentScheduler.shutdown();
        try {
            if (!currentScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                currentScheduler.shutdownNow();
            }
        } catch (InterruptedException exception) {
            currentScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public synchronized long checkpoint() {
        LogRecord beginRecord = writeAheadLogManager.append(
                LogRecordCodec.encodeBeginCheckpoint()
        );
        long beginCheckpointLsn = beginRecord.getStartLsn();

        CheckpointSnapshot snapshot =
                transactionLogManager.captureCheckpointSnapshot();
        Map<Integer, Long> dirtyPageTableEntries =
                snapshot.getDirtyPageTableEntries();
        Map<Long, ActiveTransaction> activeTransactions =
                snapshot.getActiveTransactions();

        List<Map<Integer, Long>> dptChunks =
                partition(dirtyPageTableEntries);
        for (int chunkIndex = 0; chunkIndex < dptChunks.size(); chunkIndex++) {
            writeAheadLogManager.append(
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
            writeAheadLogManager.append(
                    LogRecordCodec.encodeCheckpointAtt(
                            beginCheckpointLsn,
                            chunkIndex,
                            attChunks.get(chunkIndex)
                    )
            );
        }

        LogRecord endRecord = writeAheadLogManager.append(
                LogRecordCodec.encodeEndCheckpoint(
                        beginCheckpointLsn,
                        dptChunks.size(),
                        attChunks.size()
                )
        );
        writeAheadLogManager.flush(endRecord.getEndLsn());
        writeAheadLogManager.setCheckpointLsn(beginCheckpointLsn);
        return beginCheckpointLsn;
    }

    private void runScheduledCheckpoint() {
        try {
            checkpoint();
        } catch (RuntimeException exception) {
            LOGGER.error("Scheduled checkpoint failed", exception);
        }
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
