package com.minisql.engine.storage.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockPage implements Page {

    private int pgno;
    private byte[] data;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock rLock = lock.readLock();
    private final Lock wLock = lock.writeLock();

    public static MockPage newMockPage(int pgno, byte[] data) {
        MockPage mp = new MockPage();
        mp.pgno = pgno;
        mp.data = data;
        return mp;
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
    public void release() {}

    @Override
    public void setDirty(boolean dirty) {}

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public void setPageLsn(long pageLsn) {
        PageHeader.setPageLsn(data, pageLsn);
    }

    @Override
    public long getPageLsn() {
        return PageHeader.getPageLsn(data);
    }

    @Override
    public int getPageNumber() {
        return pgno;
    }

    @Override
    public byte[] getBytes() {
        return data;
    }
    
}
