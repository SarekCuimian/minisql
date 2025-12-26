package com.minisql.backend.dm.page;

public interface Page {

    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    void setPageLsn(long pageLsn);

    long getPageLsn();

    int getPageNumber();

    byte[] getData();

}
