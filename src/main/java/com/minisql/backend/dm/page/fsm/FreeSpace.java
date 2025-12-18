package com.minisql.backend.dm.page.fsm;

/**
 * FreeSpace 某页剩余空间大小
 */
public class FreeSpace {
    public int pgno;
    public int size;

    public FreeSpace(int pgno, int size) {
        this.pgno = pgno;
        this.size = size;
    }
}
