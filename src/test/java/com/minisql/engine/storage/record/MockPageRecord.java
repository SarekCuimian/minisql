package com.minisql.engine.storage.record;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.page.Page;

public class MockPageRecord extends PageRecord {

    private ByteSlice data;
    private byte[] oldData;
    private long uid;
    private Lock rLock;
    private Lock wLock;

    private MockPageRecord(long uid, ByteSlice data) {
        super(data, null, uid, null);
        this.data = data;
        this.oldData = new byte[data.length()];
        this.uid = uid;
        ReadWriteLock l = new ReentrantReadWriteLock();
        this.rLock = l.readLock();
        this.wLock = l.writeLock();
    }

    public static MockPageRecord newMockPageRecord(long uid, ByteSlice data) {
        return new MockPageRecord(uid, data);
    }

    public boolean isValid() {
        return true;
    }

    public ByteSlice payload() {
        return data;
    }

    public void startUpdate() {
        wLock.lock();
        System.arraycopy(data.bytes(), data.offset(), oldData, 0, oldData.length);
    }

    public void abortUpdate() {
        System.arraycopy(oldData, 0, data.bytes(), data.offset(), oldData.length);
        wLock.unlock();
    }

    public void finishUpdate(long xid) {
        wLock.unlock();
    }

    public void close() {}

    public void rLock() {
        rLock.lock();
    }

    public void rUnlock() {
        rLock.unlock();
    }

    public Page getPage() {
        return null;
    }

    public long getUid() {
        return uid;
    }

    public byte[] getBeforeImage() {
        return oldData;
    }

    public ByteSlice recordView() {
        return data;
    }
    
}
