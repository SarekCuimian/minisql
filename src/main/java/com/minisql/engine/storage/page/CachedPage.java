package com.minisql.engine.storage.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.minisql.engine.storage.page.PageBufferPool;

public class CachedPage implements Page {
    
    private final int pageNumber;
    
    private boolean dirty;

    private final byte[] bytes;

    private final Lock rLock;
    private final Lock wLock;
    
    private final PageBufferPool cache;

    public CachedPage(int pageNumber, byte[] bytes, PageBufferPool bufferPool) {
        this.pageNumber = pageNumber;
        this.bytes = bytes;
        this.cache = bufferPool;
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
        PageHeader.setPageLsn(bytes, pageLsn);
    }

    @Override
    public long getPageLsn(){
        return PageHeader.getPageLsn(bytes);
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

}
