package com.minisql.engine.storage.wal;

import java.util.Objects;

import com.minisql.engine.storage.codec.MalformedDataException;

/**
 * WAL 恢复所需的活跃事务元数据。
 *
 * <p>事务在写入 END 前都属于活跃事务，因此状态可以是 ACTIVE、COMMITTING
 * 或 ABORTING。</p>
 */
public final class ActiveTransaction {

    private final long xid;
    private final Status status;
    private final long lastLsn;

    public ActiveTransaction(long xid, Status status, long lastLsn) {
        if (xid <= 0) {
            throw new IllegalArgumentException("xid must be positive: " + xid);
        }
        if (lastLsn < LogRecord.NO_LSN) {
            throw new IllegalArgumentException(
                    "lastLsn must not be negative: " + lastLsn
            );
        }
        this.xid = xid;
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.lastLsn = lastLsn;
    }

    public long getXid() {
        return xid;
    }

    public Status getStatus() {
        return status;
    }

    public long getLastLsn() {
        return lastLsn;
    }

    public enum Status {
        ACTIVE((byte) 0),
        COMMITTING((byte) 1),
        ABORTING((byte) 2);

        private final byte code;

        Status(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static Status fromCode(byte code) {
            for (Status status : values()) {
                if (status.code == code) {
                    return status;
                }
            }
            throw new MalformedDataException(
                    "Unknown active transaction status: "
                            + Byte.toUnsignedInt(code)
            );
        }
    }
}
