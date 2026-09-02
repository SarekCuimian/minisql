package com.minisql.result;

/**
 * TBM 层执行一条 SQL 语句后返回的结构化业务结果。
 */
public class StatementResult {
    private ResultSet resultSet;
    private String message;
    private int affectedRows;
    private int resultRows;

    private StatementResult(ResultSet resultSet, String message, int affectedRows, int resultRows) {
        this.resultSet = resultSet;
        this.message = message;
        this.affectedRows = affectedRows;
        this.resultRows = resultRows;
    }

    public static StatementResult resultSet(ResultSet data) {
        int rows = data == null ? 0 : data.getRows().size();
        return new StatementResult(data, null, -1, rows);
    }

    public static StatementResult message(String message, int affectedRows) {
        return new StatementResult(null, message, affectedRows, -1);
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public String getMessage() {
        return message;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public int getResultRows() {
        return resultRows;
    }
}
