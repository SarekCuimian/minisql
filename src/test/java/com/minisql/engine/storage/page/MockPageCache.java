package com.minisql.engine.storage.page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.engine.storage.page.MockPage;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.storage.wal.LogManager;
import com.minisql.engine.storage.wal.CheckpointManager;

public class MockPageCache implements PageCache {

    private Map<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger noPages = new AtomicInteger(0);
    
    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        try {
            int pgno = noPages.incrementAndGet();
            MockPage pg = MockPage.newMockPage(pgno, initData);
            cache.put(pgno, pg);
            return pgno;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        lock.lock();
        try {
            return cache.get(pgno);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {}

    @Override
    public void releasePage(Page page) {}

    @Override
    public void setLogManager(LogManager logManager) {}

    @Override
    public void markDirtyPage(int pgno, long recLsn) {}

    @Override
    public Map<Integer, Long> snapshotDirtyPages() {
        return new HashMap<>();
    }

    @Override
    public void setCheckpointManager(CheckpointManager checkpointManager) {}

    @Override
    public void trimBadTail(int maxPgno) {}

    @Override
    public int getPageCount() {
        return noPages.intValue();
    }

    @Override
    public void persistMetaPage(Page page) {}

    @Override
    public Map<Integer, Integer> getPageFreeMap() {
        return new HashMap<>();
    }
    
}
