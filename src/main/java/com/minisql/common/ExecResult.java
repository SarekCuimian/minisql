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
    private final QueryResult queryResult;
    private final long elapsedNanos;

    private ExecResult(Type type, QueryResult queryResult, long elapsedNanos) {
        this.type = type;
        this.queryResult = queryResult;
        this.elapsedNanos = elapsedNanos;
    }


    public static ExecResult from(QueryResult queryResult, Type type, long elapsedNanos) {
        QueryResult effective = queryResult;
        if(effective == null) {
            if(type == Type.RESULT) {
                effective = QueryResult.resultSet(new ResultSet(java.util.List.of(), java.util.List.of()));
            } else {
                effective = QueryResult.message("", -1);
            }
        }
        return new ExecResult(type, effective, elapsedNanos);
    }

    public Type getType() {
        return type;
    }

    public QueryResult getOpResult() {
        return queryResult;
    }

    public long getElapsedNanos() {
        return elapsedNanos;
    }

    public ResultSet getResultSet() {
        return queryResult == null ? null : queryResult.getResultSet();
    }

    public String getMessage() {
        return queryResult == null ? "" : queryResult.getMessage();
    }

    public int getResultRows() {
        return queryResult == null ? -1 : queryResult.getResultRows();
    }

    public int getAffectedRows() {
        return queryResult == null ? -1 : queryResult.getAffectedRows();
    }
}
