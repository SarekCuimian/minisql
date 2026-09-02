package com.minisql.engine.transaction.mvcc;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.record.PageRecord;
import com.minisql.engine.storage.codec.ByteUtil;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [getBytes]
 */
public class Entry implements AutoCloseable {

    private static final int XMIN_OFFSET = 0;
    private static final int XMAX_OFFSET = XMIN_OFFSET + Long.BYTES;
    private static final int PAYLOAD_OFFSET = XMAX_OFFSET + Long.BYTES;

    private final long uid;
    private final PageRecord record;
    public Entry(PageRecord record, long uid) {
        this.uid = uid;
        this.record = record;
    }

    /**
     * 根据 XID 与业务 payload 创建完整 Entry bytes。
     * Entry 布局为 {@code [XMIN][XMAX][Payload]}。
     */
    public static byte[] newEntryBytes(long xid, byte[] payload) {
        byte[] entryBytes = new byte[PAYLOAD_OFFSET + payload.length];
        ByteUtil.putLong(entryBytes, XMIN_OFFSET, xid);
        System.arraycopy(payload, 0, entryBytes, PAYLOAD_OFFSET, payload.length);
        return entryBytes;
    }

    @Override
    public void close() {
        record.close();
    }

    /** 以拷贝形式读取 Entry 的业务 payload。 */
    public byte[] readPayload() {
        record.rLock();
        try {
            ByteSlice entryBytes = record.payload();
            byte[] payload = new byte[entryBytes.length() - PAYLOAD_OFFSET];
            System.arraycopy(
                    entryBytes.bytes(),
                    entryBytes.offset() + PAYLOAD_OFFSET,
                    payload,
                    0,
                    payload.length
            );
            return payload;
        } finally {
            record.rUnlock();
        }
    }

    /**
     * 覆盖写入业务 payload（保持 XMIN/XMAX 不变）。
     * 仅支持新 payload 长度与原 payload 长度一致的场景。
     */
    public void replacePayload(byte[] payload, long xid) {
        record.startUpdate();
        boolean updated = false;
        try {
            ByteSlice entryBytes = record.payload();
            int payloadLength = entryBytes.length() - PAYLOAD_OFFSET;
            if(payloadLength != payload.length) {
                throw new IllegalArgumentException("overwrite length mismatch");
            }
            System.arraycopy(payload, 0, entryBytes.bytes(), entryBytes.offset() + PAYLOAD_OFFSET, payload.length);
            updated = true;
        } finally {
            if (updated) {
                record.finishUpdate(xid);
            } else {
                record.abortUpdate();
            }
        }
    }

    public long getXmin() {
        record.rLock();
        try {
            ByteSlice entryBytes = record.payload();
            return ByteUtil.getLong(entryBytes.bytes(), entryBytes.offset() + XMIN_OFFSET);
        } finally {
            record.rUnlock();
        }
    }

    public long getXmax() {
        record.rLock();
        try {
            ByteSlice entryBytes = record.payload();
            return ByteUtil.getLong(entryBytes.bytes(), entryBytes.offset() + XMAX_OFFSET);
        } finally {
            record.rUnlock();
        }
    }

    public void markDeleted(long xid) {
        record.startUpdate();
        boolean updated = false;
        try {
            ByteSlice entryBytes = record.payload();
            ByteUtil.putLong(entryBytes.bytes(), entryBytes.offset() + XMAX_OFFSET, xid);
            updated = true;
        } finally {
            if (updated) {
                record.finishUpdate(xid);
            } else {
                record.abortUpdate();
            }
        }
    }

    public long getUid() {
        return uid;
    }

}
