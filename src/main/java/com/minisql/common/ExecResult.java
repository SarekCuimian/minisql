package com.minisql.common;

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
    private final OpResult opResult;
    private final long elapsedNanos;

    private ExecResult(Type type, OpResult opResult, long elapsedNanos) {
        this.type = type;
        this.opResult = opResult;
        this.elapsedNanos = elapsedNanos;
    }

    public static ExecResult from(OpResult opResult, Type type, long elapsedNanos) {
        OpResult effective = opResult;
        if(effective == null) {
            if(type == Type.RESULT) {
                effective = OpResult.resultSet(new ResultSet(java.util.List.of(), java.util.List.of()));
            } else {
                effective = OpResult.message("", -1);
            }
        }
        return new ExecResult(type, effective, elapsedNanos);
    }

    public Type getType() {
        return type;
    }

    public OpResult getOpResult() {
        return opResult;
    }

    public ResultSet getResultSet() {
        return opResult == null ? null : opResult.getResultSet();
    }

    public String getMessage() {
        return opResult == null ? "" : opResult.getMessage();
    }

    public long getElapsedNanos() {
        return elapsedNanos;
    }

    public int getResultRows() {
        return opResult == null ? -1 : opResult.getResultRows();
    }

    public int getAffectedRows() {
        return opResult == null ? -1 : opResult.getAffectedRows();
    }
}
