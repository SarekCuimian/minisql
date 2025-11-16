package com.minisql.backend.utils.format;

/**
 * 数据库执行后的结构化结果
 * 供不同的输出层自定义格式化逻辑
 */
public class ExecResult {

    public enum Type {
        RESULT,
        OK
    }

    private final Type type;
    private final byte[] payload;
    private final long elapsedNanos;
    private final int resultRows;
    private final int affectedRows;

    private ExecResult(Type type, byte[] payload, long elapsedNanos, int resultRows, int affectedRows) {
        this.type = type;
        this.payload = payload;
        this.elapsedNanos = elapsedNanos;
        this.resultRows = resultRows;
        this.affectedRows = affectedRows;
    }

    public static ExecResult resultSet(byte[] payload, long elapsedNanos, int rows) {
        return new ExecResult(Type.RESULT, payload, elapsedNanos, rows, -1);
    }

    public static ExecResult okPacket(byte[] payload, long elapsedNanos, int affectedRows) {
        return new ExecResult(Type.OK, payload, elapsedNanos, -1, affectedRows);
    }

    public Type getType() {
        return type;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getElapsedNanos() {
        return elapsedNanos;
    }

    public int getResultRows() {
        return resultRows;
    }

    public int getAffectedRows() {
        return affectedRows;
    }
}
