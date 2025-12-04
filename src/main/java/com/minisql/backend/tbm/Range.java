package com.minisql.backend.tbm;

/**
 * 索引扫描用的闭区间 [left, right]。
 */
public class Range {
    private final long left;
    private final long right;

    public Range(long left, long right) {
        this.left = left;
        this.right = right;
    }

    public long getLeft() {
        return left;
    }

    public long getRight() {
        return right;
    }
}
