package com.minisql.engine.storage.page;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.minisql.engine.cache.AbstractCache;
import com.minisql.engine.storage.wal.WriteAheadLogger;
import com.minisql.engine.storage.wal.CheckpointManager;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.CachedPage;
import com.minisql.engine.storage.io.FileChannelUtil;
import com.minisql.error.Panic;
import com.minisql.error.Error;

public final class PageBufferPool extends AbstractCache<Page> implements AutoCloseable {

    // Constants
    public static final int PAGE_SIZE = 1 << 13;
    private static final int MIN_CACHE_PAGE_COUNT = 10;
    private static final long FLUSH_INTERVAL_MS = 1000L;
    private static final int MAX_PAGES_PER_BATCH = 64;
    public static final String DB_SUFFIX = ".db";

    // Fields
    private final RandomAccessFile file;
    private final FileChannel fc;
    /**
     * Coordinates access to the database file.
     * Normal positioned reads and writes share the read lock; file truncation
     * and close take the write lock exclusively.
     */
    private final ReadWriteLock accessLock;
    /** Serializes page-number allocation with initial-page persistence and publication. */
    private final Lock allocationLock;
    /** Serializes FileChannel.force() without serializing normal positioned I/O. */
    private final Lock forceLock;
    private final AtomicInteger pageNumberCounter;

    private final DirtyPageTable dirtyPageTable = new DirtyPageTable();
    private final AtomicBoolean cleanerStarted = new AtomicBoolean(false);
    private volatile boolean cleanerRunning;
    private Thread pageCleaner;

    private volatile boolean closing;
    private volatile WriteAheadLogger walLogger;
    private volatile CheckpointManager checkpointManager;

    private final ReentrantLock cleanerLock = new ReentrantLock();
    private final Condition hasDirtyPage = cleanerLock.newCondition();

    // Constructor
    private PageBufferPool(
            RandomAccessFile file,
            FileChannel fileChannel,
            int capacity
    ) {
        super(capacity);
        if (capacity < MIN_CACHE_PAGE_COUNT) {
            Panic.of(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.of(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.accessLock = new ReentrantReadWriteLock();
        this.allocationLock = new ReentrantLock();
        this.forceLock = new ReentrantLock();
        this.pageNumberCounter = new AtomicInteger((int) length / PAGE_SIZE);
    }

    public static PageBufferPool create(String path, long memory) {
        File file = new File(path + DB_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.of(Error.FileExistsException);
            }
        } catch (IOException exception) {
            Panic.of(exception);
        }
        requireReadableAndWritable(file);
        return openFile(file, memory);
    }

    public static PageBufferPool open(String path, long memory) {
        File file = new File(path + DB_SUFFIX);
        if (!file.exists()) {
            Panic.of(Error.FileNotExistsException);
        }
        requireReadableAndWritable(file);
        return openFile(file, memory);
    }

    private static PageBufferPool openFile(File file, long memory) {
        try {
            RandomAccessFile randomAccessFile =
                    new RandomAccessFile(file, "rw");
            return new PageBufferPool(
                    randomAccessFile,
                    randomAccessFile.getChannel(),
                    (int) (memory / PAGE_SIZE)
            );
        } catch (FileNotFoundException exception) {
            Panic.of(exception);
            throw new IllegalStateException(
                    "Unable to open page file: " + file,
                    exception
            );
        }
    }

    private static void requireReadableAndWritable(File file) {
        if (!file.canRead() || !file.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }
    }

    // Dependency injection
    public void setWalLogger(WriteAheadLogger walLogger) {
        this.walLogger = walLogger;
        // 启动 PageCleaner 线程
        startPageCleaner();
    }
    public void setCheckpointManager(CheckpointManager checkpointManager) {
        this.checkpointManager = checkpointManager;
    }

    // Public API
    /** 新建 page */
    public int newPage(byte[] initData) {
        allocationLock.lock();
        try {
            if (closing) {
                throw new IllegalStateException("Page cache is closed");
            }
            int pgno = pageNumberCounter.get() + 1;
            Page pg = new CachedPage(pgno, initData, this);
            persist(pg);
            // Only publish the new page number after its full initial image is durable.
            pageNumberCounter.set(pgno);
            return pgno;
        } finally {
            allocationLock.unlock();
        }
    }

    /** 从缓存获取 page */
    public Page getPage(int pgno) throws Exception {
        return get(pgno);
    }

    /** 归还 page 引用到缓存 */
    public void releasePage(Page page) {
        release(page.getPageNumber());
    }

    /** 持久化 MetaPage */
    public void persistMetaPage(Page page) {
        persist(page);
    }

    public void trimBadTail(int maxPgno) {
        // Must be called only when no page references are held; otherwise later flush can extend file.
        long size = getPageOffset(maxPgno + 1);
        allocationLock.lock();
        try {
            accessLock.writeLock().lock();
            try {
                file.setLength(size);
                pageNumberCounter.set(maxPgno);
            } catch (IOException e) {
                Panic.of(e);
            } finally {
                accessLock.writeLock().unlock();
            }
        } finally {
            allocationLock.unlock();
        }
    }

    public int getPageCount() {
        return pageNumberCounter.intValue();
    }

    /**
     * 直接扫描文件中获取页号与空闲空间大小
     * @return 所有页号与空闲空间大小的 map
     */
    public Map<Integer, Integer> getPageFreeMap() {
        int pageCount = getPageCount();
        Map<Integer, Integer> map = new HashMap<>(Math.max(16, pageCount));
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        accessLock.readLock().lock();
        try {
            for (int pgno = 2; pgno <= pageCount; pgno++) {
                buf.clear();
                FileChannelUtil.readFully(fc, buf, getPageOffset(pgno));
                map.put(pgno, DataPage.getFreeSpaceSize(buf.array()));
            }
        } catch (IOException e) {
            Panic.of(e);
        } finally {
            accessLock.readLock().unlock();
        }
        return map;
    }
    protected boolean isEvictable(Page pg) {
        // 脏页不允许被直接淘汰
        return !pg.isDirty();
    }

    /**
     * 根据 pageNumber 从数据库文件中读取页数据，并包裹成 Page
     */
    protected Page loadCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = getPageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        accessLock.readLock().lock();
        try {
            if (closing) {
                throw new IllegalStateException("Page cache is closed");
            }
            FileChannelUtil.readFully(fc, buf, offset);
        } catch (IOException e) {
            Panic.of(e);
        } finally {
            accessLock.readLock().unlock();
        }
        return new CachedPage(pgno, buf.array(), this);
    }
    protected void flushCache(Page pg) {
        // 该路径暂时不参与刷脏页 
        return;
    }
    public void markDirtyPage(int pgno, long recLsn) {
        boolean firstDirtied = dirtyPageTable.mark(pgno, recLsn);
        // 当第一次页变脏时候触发
        if (firstDirtied) {
            cleanerLock.lock();
            try {
                hasDirtyPage.signal();
            }finally {
                cleanerLock.unlock();
            }
        }
    }
    public Map<Integer, Long> snapshotDirtyPages() {
        return dirtyPageTable.snapshot();
    }

    /** 启动 PageCleaner 线程 */
    private void startPageCleaner() {
        if (cleanerStarted.compareAndSet(false, true)) {
            cleanerRunning = true;
            pageCleaner = new Thread(new PageCleaner(), "PageCleaner");
            pageCleaner.start();
        }
    }
    /** 停止 PageCleaner 线程 */
    private void stopPageCleaner() {
        cleanerLock.lock();
        try {
            cleanerRunning = false;
            // Wake a cleaner waiting for dirty pages. Do not interrupt the thread:
            // it may currently be blocked in a WAL flush that must complete safely.
            hasDirtyPage.signalAll();
        } finally {
            cleanerLock.unlock();
        }
        if (pageCleaner != null) {
            try {
                pageCleaner.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // A clean close must persist every tracked dirty page even if the
        // background cleaner had not consumed the final signal yet.
        PageCleaner finalCleaner = new PageCleaner();
        List<PageSnapshot> snapshots = new ArrayList<>(MAX_PAGES_PER_BATCH);
        while (!dirtyPageTable.isEmpty()) {
            finalCleaner.batchFlush(snapshots);
        }
    }


    private final class PageCleaner implements Runnable {
        public void run() {
            List<PageSnapshot> candidates = new ArrayList<>(MAX_PAGES_PER_BATCH);
            while (cleanerRunning) {
                if (!awaitDirtyPages()) {
                    return;
                }
                try {
                    batchFlush(candidates);
                } catch (Exception e) {
                    Panic.of(e);
                }
                if (!dirtyPageTable.isEmpty()) {
                    try {
                        Thread.sleep(FLUSH_INTERVAL_MS);
                    } catch (InterruptedException e) {
                        if (!cleanerRunning) {
                            return;
                        }
                    }
                }
            }
        }

        /** 等待 dirty 页 */
        private boolean awaitDirtyPages() {
            cleanerLock.lock();
            try {
                while (cleanerRunning && dirtyPageTable.isEmpty()) {
                    hasDirtyPage.await();
                }
                return cleanerRunning;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                cleanerLock.unlock();
            }
        }

        /** 批量刷脏页 */
        private void batchFlush(List<PageSnapshot> snapshots) {
            snapshots.clear();
            List<DirtyPageTable.DirtyPage> pages =
                    dirtyPageTable.getBatch(MAX_PAGES_PER_BATCH);
            int flushedPages = 0;
            for (DirtyPageTable.DirtyPage page : pages) {
                if (flushedPages >= MAX_PAGES_PER_BATCH) {
                    break;
                }
                PageSnapshot candidate = flushPage(
                        page.getPageNumber(),
                        page.getRecoveryLsn()
                );
                if (candidate != null) {
                    snapshots.add(candidate);
                    flushedPages++;
                }
            }
            if (!snapshots.isEmpty()) {
                // 合并 force
                accessLock.readLock().lock();
                try {
                    forceFile();
                } finally {
                    accessLock.readLock().unlock();
                }
                for (PageSnapshot snapshot : snapshots) {
                    // 比较页快照是否有更新，若无更新，则标记页为干净
                    compareAndClean(snapshot);
                }
            }
            updateCheckpoint();
        }
    }

    private static final class PageSnapshot {
        final int pgno;
        final long pageLsn;
        final long recLsn;

        PageSnapshot(int pgno, long pageLsn, long recLsn) {
            this.pgno = pgno;
            this.pageLsn = pageLsn;
            this.recLsn = recLsn;
        }
    }

    /**
     * 创建页快照，并写入 FileChannel
     * @param pgno 页号
     * @param recLsn 使干净页首次变脏的日志起始 LSN
     * @return 页快照
     */
    private PageSnapshot flushPage(int pgno, long recLsn) {
        // Avoid LRU touch during background flush.
        Page pg = lookup(pgno);
        if (pg == null) {
            return null;
        }
        try {
            return writePageSnapshot(pg, recLsn);
        } finally {
            pg.release();
        }
    }

    private PageSnapshot writePageSnapshot(Page pg, long recLsn) {
        int pgno = pg.getPageNumber();
        while (true) {
            long snapshotPageLsn;
            byte[] snapshotPageBytes = null;
            boolean needLogFlush = false;

            pg.wLock();
            try {
                if (!pg.isDirty()) {
                    dirtyPageTable.remove(pgno, recLsn);
                    return null;
                }
                snapshotPageLsn = pg.getPageLsn();
                long flushedLsn = walLogger.getFlushedLsn();
                if (snapshotPageLsn > flushedLsn) {
                    needLogFlush = true;
                } else {
                    snapshotPageBytes = Arrays.copyOf(pg.getBytes(), PAGE_SIZE);
                }
            } finally {
                pg.wUnlock();
            }

            // 锁外阻塞式推进日志刷盘，保证 WAL
            if (needLogFlush) {
                walLogger.flush(snapshotPageLsn);
                continue;
            }
            // 文件锁内写入文件，暂不 force，后续合并 force
            long offset = getPageOffset(pgno);
            accessLock.readLock().lock();
            try {
                ByteBuffer buf = ByteBuffer.wrap(snapshotPageBytes);
                FileChannelUtil.writeFully(fc, buf, offset);
            } catch (IOException e) {
                Panic.of(e);
            } finally {
                accessLock.readLock().unlock();
            }
            
            return new PageSnapshot(pgno, snapshotPageLsn, recLsn);
        }
    }

    /** 比较页快照，若无更新，则从 DPT 中移除并标记页为干净， */
    private void compareAndClean(PageSnapshot snapshot) {
        Page pg = lookup(snapshot.pgno);
        if (pg == null) return;
        pg.wLock();
        try {
            if (pg.isDirty() && pg.getPageLsn() == snapshot.pageLsn) {
                pg.setDirty(false);
                dirtyPageTable.remove(snapshot.pgno, snapshot.recLsn);
                return;
            }
            if (!pg.isDirty()) {
                dirtyPageTable.remove(snapshot.pgno, snapshot.recLsn);
            }
        } finally {
            pg.wUnlock();
            pg.release();
        }
    }

    /** 更新 checkpoint */
    private void updateCheckpoint() {
        CheckpointManager manager = checkpointManager;
        if (manager != null) {
            manager.checkpoint();
        }
    }

    // =========================
    // IO helpers
    // =========================

    /** 将 page 持久化写入文件 */
    private void persist(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = getPageOffset(pgno);

        pg.wLock();
        try {
            accessLock.readLock().lock();
            try {
                ByteBuffer buf = ByteBuffer.wrap(pg.getBytes());
                FileChannelUtil.writeFully(fc, buf, offset);
                forceFile();
                pg.setDirty(false);
            } catch (IOException e) {
                Panic.of(e);
            } finally {
                accessLock.readLock().unlock();
            }
        } finally {
            pg.wUnlock();
        }
    }

    /** 获取页对应的文件偏移量 */
    private long getPageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }

    /**
     * Forces file contents to stable storage. The caller must hold
     * {@code accessLock.readLock()} so close and truncate cannot race this operation.
     */
    private void forceFile() {
        forceLock.lock();
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.of(e);
        } finally {
            forceLock.unlock();
        }
    }
    public void close() {
        allocationLock.lock();
        try {
            closing = true;
            stopPageCleaner();
            updateCheckpoint();
            super.close();
            accessLock.writeLock().lock();
            try {
                fc.close();
                file.close();
            } catch (IOException e) {
                Panic.of(e);
            } finally {
                accessLock.writeLock().unlock();
            }
        } finally {
            allocationLock.unlock();
        }
    }

}
