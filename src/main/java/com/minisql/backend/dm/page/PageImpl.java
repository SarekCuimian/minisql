package com.minisql.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.dm.page.cache.PageCache;

public class PageImpl implements Page {
    private final int pageNumber;
    private final byte[] data;
    private boolean dirty;
    /** 该页对应的最新修改的 LSN */
    private long pageLsn;
    private final Lock lock;
    
    private final PageCache cache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.cache = pageCache;
        this.lock = new ReentrantLock();
    }
    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
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
    public byte[] getData() {
        return data;
    }

}
