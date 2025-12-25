package com.minisql.backend.common;

public class MockCache extends AbstractCache<Long> {

    public MockCache(int capacity) {
        super(capacity);
    }

    @Override
    protected Long loadCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void flushCache(Long obj) {}
    
}
