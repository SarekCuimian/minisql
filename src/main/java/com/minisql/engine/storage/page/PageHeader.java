package com.minisql.engine.storage.page;

import com.minisql.engine.storage.codec.ByteUtil;

/**
 * 所有数据库页共享的持久化页头。
 *
 * <p>page LSN 是最后一次修改该页的完整日志记录 {@code endLsn}。</p>
 */
public final class PageHeader {

    public static final int PAGE_LSN_OFFSET = 0;
    public static final int HEADER_SIZE = Long.BYTES;

    private PageHeader() {
    }

    public static long getPageLsn(byte[] pageBytes) {
        return ByteUtil.getLong(pageBytes, PAGE_LSN_OFFSET);
    }

    public static void setPageLsn(byte[] pageBytes, long pageLsn) {
        if (pageLsn < 0) {
            throw new IllegalArgumentException("pageLsn must not be negative");
        }
        ByteUtil.putLong(pageBytes, PAGE_LSN_OFFSET, pageLsn);
    }
}
