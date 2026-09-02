package com.minisql.engine.storage.wal;

import java.util.Objects;

/**
 * WAL 中的一条完整物理记录，由其文件区间和 payload 组成。
 */
public final class LogRecord {

    public static final long NO_LSN = 0L;

    private final long startLsn;
    private final long endLsn;
    private final byte[] payload;

    public LogRecord(long startLsn, long endLsn, byte[] payload) {
        if (startLsn < 0) {
            throw new IllegalArgumentException("startLsn must not be negative");
        }
        if (endLsn <= startLsn) {
            throw new IllegalArgumentException("endLsn must be greater than startLsn");
        }
        this.startLsn = startLsn;
        this.endLsn = endLsn;
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
    }

    public long getStartLsn() {
        return startLsn;
    }

    public long getEndLsn() {
        return endLsn;
    }

    public long length() {
        return endLsn - startLsn;
    }

    public byte[] getPayload() {
        return payload;
    }

    public LogRecordType getType() {
        return LogRecordCodec.decodeType(payload);
    }
}
