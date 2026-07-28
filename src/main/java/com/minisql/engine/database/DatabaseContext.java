package com.minisql.engine.database;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.minisql.engine.storage.page.PageBufferPool;
import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.storage.wal.ActiveTransactionTable;
import com.minisql.engine.storage.wal.CheckpointManager;
import com.minisql.engine.storage.wal.TransactionLogManager;
import com.minisql.engine.storage.wal.WriteAheadLogManager;
import com.minisql.engine.table.TableManager;
import com.minisql.engine.transaction.mvcc.VersionManager;
import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.transaction.xid.XidStatusTable;

/**
 * 封装单个数据库实例关联的 TM/DM/VM/TBM 组件，
 * 并用引用计数控制其生命周期，支持在多个连接之间复用。
 */
public final class DatabaseContext implements AutoCloseable {

    private final String name;
    private final XidAllocator xidAllocator;
    private final WriteAheadLogManager writeAheadLogManager;
    private final PageBufferPool bufferPool;
    private final CheckpointManager checkpointManager;
    private final PageRecordManager pageRecordManager;
    private final TableManager tbm;

    /** 当前有多少个 Executor 持有这个上下文（连接/会话等） */
    private final AtomicInteger refCount = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DatabaseContext(
            String name,
            XidAllocator xidAllocator,
            WriteAheadLogManager writeAheadLogManager,
            PageBufferPool bufferPool,
            CheckpointManager checkpointManager,
            PageRecordManager pageRecordManager,
            TableManager tbm
    ) {
        this.name = name;
        this.xidAllocator = xidAllocator;
        this.writeAheadLogManager = writeAheadLogManager;
        this.bufferPool = bufferPool;
        this.checkpointManager = checkpointManager;
        this.pageRecordManager = pageRecordManager;
        this.tbm = tbm;
    }

    static DatabaseContext create(String name, String basePath, long memory) throws Exception {
        XidAllocator xidAllocator = XidAllocator.create(basePath);
        WriteAheadLogManager writeAheadLogManager = null;
        PageBufferPool bufferPool = null;
        CheckpointManager checkpointManager = null;
        PageRecordManager pageRecordManager = null;
        try {
            XidStatusTable xidStatusTable = new XidStatusTable(xidAllocator);
            ActiveTransactionTable activeTransactionTable = new ActiveTransactionTable();
            writeAheadLogManager =
                    WriteAheadLogManager.create(basePath);
            bufferPool = PageBufferPool.create(
                    basePath,
                    memory,
                    writeAheadLogManager
            );
            TransactionLogManager transactionLogManager =
                    new TransactionLogManager(
                            writeAheadLogManager,
                            activeTransactionTable,
                            xidStatusTable,
                            bufferPool
                    );
            checkpointManager = new CheckpointManager(
                    writeAheadLogManager,
                    transactionLogManager
            );
            pageRecordManager = PageRecordManager.create(
                    bufferPool,
                    transactionLogManager
            );
            VersionManager versionManager = new VersionManager(
                    xidAllocator,
                    xidStatusTable,
                    pageRecordManager,
                    transactionLogManager
            );
            TableManager tableManager = TableManager.create(
                    basePath,
                    versionManager,
                    pageRecordManager
            );
            DatabaseContext context = new DatabaseContext(
                    name,
                    xidAllocator,
                    writeAheadLogManager,
                    bufferPool,
                    checkpointManager,
                    pageRecordManager,
                    tableManager
            );
            context.startBackgroundServices();
            return context;
        } catch (Exception exception) {
            closePartiallyCreated(
                    checkpointManager,
                    pageRecordManager,
                    bufferPool,
                    writeAheadLogManager,
                    xidAllocator,
                    exception
            );
            throw exception;
        }
    }

    static DatabaseContext open(String name, String basePath, long memory) throws Exception {
        XidAllocator xidAllocator = XidAllocator.open(basePath);
        WriteAheadLogManager writeAheadLogManager = null;
        PageBufferPool bufferPool = null;
        CheckpointManager checkpointManager = null;
        PageRecordManager pageRecordManager = null;
        try {
            XidStatusTable xidStatusTable =
                    new XidStatusTable(xidAllocator);
            ActiveTransactionTable activeTransactionTable =
                    new ActiveTransactionTable();
            writeAheadLogManager =
                    WriteAheadLogManager.open(basePath);
            bufferPool = PageBufferPool.open(
                    basePath,
                    memory,
                    writeAheadLogManager
            );
            TransactionLogManager transactionLogManager =
                    new TransactionLogManager(
                            writeAheadLogManager,
                            activeTransactionTable,
                            xidStatusTable,
                            bufferPool
                    );
            checkpointManager = new CheckpointManager(
                    writeAheadLogManager,
                    transactionLogManager
            );
            pageRecordManager = PageRecordManager.open(
                    bufferPool,
                    transactionLogManager,
                    xidStatusTable,
                    writeAheadLogManager
            );
            VersionManager versionManager = new VersionManager(
                    xidAllocator,
                    xidStatusTable,
                    pageRecordManager,
                    transactionLogManager
            );
            TableManager tableManager = TableManager.open(
                    basePath,
                    versionManager,
                    pageRecordManager
            );
            DatabaseContext context = new DatabaseContext(
                    name,
                    xidAllocator,
                    writeAheadLogManager,
                    bufferPool,
                    checkpointManager,
                    pageRecordManager,
                    tableManager
            );
            context.startBackgroundServices();
            return context;
        } catch (Exception exception) {
            closePartiallyCreated(
                    checkpointManager,
                    pageRecordManager,
                    bufferPool,
                    writeAheadLogManager,
                    xidAllocator,
                    exception
            );
            throw exception;
        }
    }

    public String getName() {
        return name;
    }

    public TableManager getTableManager() {
        return tbm;
    }

    /** 增加一次引用：某个连接/会话开始使用这个数据库实例 */
    public void retain() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Database context is already closed: " + name
            );
        }
        refCount.incrementAndGet();
    }

    /** 释放一次引用：连接/会话结束使用 */
    public void release() {
        int count = refCount.decrementAndGet();
        if (count < 0) {
            refCount.set(0);
            throw new IllegalStateException(
                    "Database context released too many times: " + name
            );
        }
    }

    /** 是否仍有连接在使用这个数据库实例 */
    public boolean inUse() {
        return refCount.get() > 0;
    }

    /**
     * 关闭底层组件，仅应由 DatabaseManager 在确定不再使用该实例时调用。
     * 普通调用方不要直接调用 close()。
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        List<RuntimeException> failures = new ArrayList<>();
        runCloseStep(checkpointManager::stop, failures);
        runCloseStep(pageRecordManager::close, failures);
        runCloseStep(bufferPool::close, failures);
        if (failures.isEmpty()) {
            // PageCleaner 已停止且脏页全部持久化，再记录最终 checkpoint。
            runCloseStep(checkpointManager::checkpoint, failures);
        }
        runCloseStep(writeAheadLogManager::close, failures);
        runCloseStep(xidAllocator::close, failures);

        throwIfCloseFailed(failures);
    }

    private void startBackgroundServices() {
        bufferPool.start();
        checkpointManager.start();
    }

    private static void closePartiallyCreated(
            CheckpointManager checkpointManager,
            PageRecordManager pageRecordManager,
            PageBufferPool bufferPool,
            WriteAheadLogManager writeAheadLogManager,
            XidAllocator xidAllocator,
            Exception failure
    ) {
        List<RuntimeException> closeFailures = new ArrayList<>();
        if (checkpointManager != null) {
            runCloseStep(checkpointManager::stop, closeFailures);
        }
        if (pageRecordManager != null) {
            runCloseStep(pageRecordManager::close, closeFailures);
        }
        if (bufferPool != null) {
            runCloseStep(bufferPool::close, closeFailures);
        }
        if (writeAheadLogManager != null) {
            runCloseStep(writeAheadLogManager::close, closeFailures);
        }
        runCloseStep(xidAllocator::close, closeFailures);
        for (RuntimeException closeFailure : closeFailures) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static void runCloseStep(Runnable closeStep, List<RuntimeException> failures) {
        try {
            closeStep.run();
        } catch (RuntimeException closeFailure) {
            failures.add(closeFailure);
        }
    }

    private static void throwIfCloseFailed(List<RuntimeException> failures) {
        if (failures.isEmpty()) {
            return;
        }

        RuntimeException failure = failures.get(0);
        for (int index = 1; index < failures.size(); index++) {
            failure.addSuppressed(failures.get(index));
        }
        throw failure;
    }
}
