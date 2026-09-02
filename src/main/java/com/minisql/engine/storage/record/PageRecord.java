package com.minisql.engine.storage.record;

import java.util.Arrays;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.codec.ByteUtil;
import com.minisql.engine.storage.codec.UidUtil;
import com.minisql.engine.storage.page.Page;

/**
 * Data Page 中一条 physical record 的短生命周期访问句柄。
 *
 * <p>页内布局为 {@code [valid flag][payload length][payload]}。
 * 句柄存活期间持有所在
 * Page 的引用，调用方使用完毕后必须调用 {@link #close()} 归还该引用。</p>
 */
public class PageRecord implements AutoCloseable {

    /** valid flag 的字节偏移量。 */
    static final int VALID_FLAG_OFFSET = 0;
    /** payload length 字段的字节偏移量。 */
    static final int PAYLOAD_LENGTH_OFFSET = 1;
    /** payload 的字节偏移量。 */
    static final int PAYLOAD_OFFSET = 3;

    /** 指向 Page 内完整 physical record 的字节视图，包含 Record header 与 payload。 */
    private final ByteSlice recordBytes;
    private final PageRecordManager pageRecordManager;
    private final long uid;
    private final Page page;
    private byte[] beforeImage;

    public PageRecord(ByteSlice recordBytes, Page page, long uid, PageRecordManager pageRecordManager) {
        this.recordBytes = recordBytes;
        this.page = page;
        this.uid = uid;
        this.pageRecordManager = pageRecordManager;
    }

    public boolean isValid() {
        return recordBytes.bytes()[recordBytes.offset() + VALID_FLAG_OFFSET] == 0;
    }

    public ByteSlice payload() {
        return recordBytes.slice(PAYLOAD_OFFSET, recordBytes.length() - PAYLOAD_OFFSET);
    }

    /** 获取 Page 写锁，并生成用于 WAL 与回滚的 before image。 */
    public void startUpdate() {
        page.wLock();
        try {
            page.setDirty(true);
            beforeImage = Arrays.copyOfRange(recordBytes.bytes(), recordBytes.offset(), recordBytes.end());
        } catch (RuntimeException e) {
            page.wUnlock();
            throw e;
        }
    }

    /** 写入 update log 并释放 Page 写锁。 */
    public void finishUpdate(long xid) {
        try {
            pageRecordManager.logRecordUpdate(xid, this);
        } finally {
            beforeImage = null;
            page.wUnlock();
        }
    }

    /** 如有 before image 则恢复它，然后释放 Page 写锁。 */
    public void abortUpdate() {
        try {
            if (beforeImage != null) {
                System.arraycopy(beforeImage, 0, recordBytes.bytes(), recordBytes.offset(), beforeImage.length);
            }
        } finally {
            beforeImage = null;
            page.wUnlock();
        }
    }

    @Override
    public void close() {
        page.release();
    }

    /** 获取该记录所在 Page 的 shared lock。 */
    public void rLock() {
        page.rLock();
    }

    /** 释放该记录所在 Page 的 shared lock。 */
    public void rUnlock() {
        page.rUnlock();
    }

    public Page getPage() {
        return page;
    }

    public long getUid() {
        return uid;
    }

    public byte[] getBeforeImage() {
        return beforeImage;
    }

    /** 返回完整 physical record 的字节视图，包含 Record header 与 payload。 */
    public ByteSlice recordView() {
        return recordBytes;
    }

    /** 根据 payload 创建包含 Record header 的完整 record bytes。 */
    public static byte[] newRecordBytes(byte[] payload) {
        byte[] recordBytes = new byte[PAYLOAD_OFFSET + payload.length];
        ByteUtil.putShort(recordBytes, PAYLOAD_LENGTH_OFFSET, (short) payload.length);
        System.arraycopy(payload, 0, recordBytes, PAYLOAD_OFFSET, payload.length);
        return recordBytes;
    }

    /** 从 {@code offset} 解析一条 physical record；返回的句柄持有 {@code page} 的引用。 */
    public static PageRecord parse(Page page, short offset, PageRecordManager pageRecordManager) {
        byte[] pageBytes = page.getBytes();
        short payloadLength = ByteUtil.getShort(pageBytes, offset + PAYLOAD_LENGTH_OFFSET);
        short recordByteLength = (short) (payloadLength + PAYLOAD_OFFSET);
        long uid = UidUtil.getUid(page.getPageNumber(), offset);
        return new PageRecord(new ByteSlice(pageBytes, offset, recordByteLength), page, uid, pageRecordManager);
    }

    /** 将完整 record bytes 的 valid flag 标记为无效。 */
    public static void markInvalid(byte[] recordBytes) {
        recordBytes[VALID_FLAG_OFFSET] = 1;
    }
}
