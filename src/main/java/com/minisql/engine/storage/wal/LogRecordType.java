package com.minisql.engine.storage.wal;

import com.minisql.engine.storage.codec.MalformedDataException;

/**
 * WAL 逻辑记录类型。
 *
 * <p>磁盘编码显式指定，不依赖 enum ordinal，避免调整枚举顺序破坏已有格式。</p>
 */
public enum LogRecordType {
    INSERT((byte) 0),
    UPDATE((byte) 1),
    BEGIN((byte) 2),
    COMMIT((byte) 3),
    ABORT((byte) 4),
    END((byte) 5),
    CLR((byte) 6),
    BEGIN_CHECKPOINT((byte) 7),
    CHECKPOINT_DPT((byte) 8),
    CHECKPOINT_ATT((byte) 9),
    END_CHECKPOINT((byte) 10);

    private final byte code;

    LogRecordType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static LogRecordType fromCode(byte code) {
        for (LogRecordType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new MalformedDataException(
                "Unknown WAL record type: " + Byte.toUnsignedInt(code)
        );
    }

    public boolean isPageChange() {
        return this == INSERT || this == UPDATE;
    }

    public boolean isTransactionStateChange() {
        return this == BEGIN || this == COMMIT || this == ABORT || this == END;
    }

    public boolean isRedoablePageChange() {
        return isPageChange() || this == CLR;
    }

    public boolean isCheckpoint() {
        return this == BEGIN_CHECKPOINT
                || this == CHECKPOINT_DPT
                || this == CHECKPOINT_ATT
                || this == END_CHECKPOINT;
    }
}
