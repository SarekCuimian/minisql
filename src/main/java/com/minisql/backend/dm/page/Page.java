package com.minisql.backend.dm.page;

public interface Page {

    void wLock();

    void wUnlock();

    void rLock();

    void rUnlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    void setPageLsn(long pageLsn);

    long getPageLsn();

    int getPageNumber();

    byte[] getBytes();

}
