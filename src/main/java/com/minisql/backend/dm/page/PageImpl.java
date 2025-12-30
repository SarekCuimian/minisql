package com.minisql.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.minisql.backend.dm.page.cache.PageCache;

public class PageImpl implements Page {
    
    private final int pageNumber;
    
    private boolean dirty;

    /** 该页对应的最新修改的 LSN */
    private long pageLsn;

    private long recLsn;

    private final byte[] bytes;

    private final Lock rLock;
    private final Lock wLock;
    
    private final PageCache cache;

    public PageImpl(int pageNumber, byte[] bytes, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.bytes = bytes;
        this.cache = pageCache;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
    }
    @Override
    public void wLock() {
        wLock.lock();
    }

    @Override
    public void wUnlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnlock() {
        rLock.unlock();
    }

    @Override
    public void release() {
        cache.releasePage(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public void setPageLsn(long pageLsn){
        this.pageLsn = pageLsn;
    };

    @Override
    public long getPageLsn(){
        return pageLsn;
    };

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

}
