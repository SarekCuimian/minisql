package com.minisql.result;

/**
 * 数据库执行后的结构化结果，供不同输出层自定义格式化逻辑。
 */
public class ExecutionResult {

    public enum Type {
        RESULT,
        OK
    }

    private final Type type;
    private final StatementResult statementResult;
    private final long elapsedNanos;

    private ExecutionResult(Type type, StatementResult statementResult, long elapsedNanos) {
        this.type = type;
        this.statementResult = statementResult;
        this.elapsedNanos = elapsedNanos;
    }

    public static ExecutionResult from(StatementResult statementResult, Type type, long elapsedNanos) {
        StatementResult effective = statementResult;
        if(effective == null) {
            if(type == Type.RESULT) {
                effective = StatementResult.resultSet(new ResultSet(java.util.List.of(), java.util.List.of()));
            } else {
                effective = StatementResult.message("", -1);
            }
        }
        return new ExecutionResult(type, effective, elapsedNanos);
    }

    public Type getType() {
        return type;
    }

    public StatementResult getStatementResult() {
        return statementResult;
    }

    public long getElapsedNanos() {
        return elapsedNanos;
    }

    public ResultSet getResultSet() {
        return statementResult == null ? null : statementResult.getResultSet();
    }

    public String getMessage() {
        return statementResult == null ? "" : statementResult.getMessage();
    }

    public int getResultRows() {
        return statementResult == null ? -1 : statementResult.getResultRows();
    }

    public int getAffectedRows() {
        return statementResult == null ? -1 : statementResult.getAffectedRows();
    }
}
