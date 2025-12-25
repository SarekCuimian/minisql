package com.minisql.common;

/**
 * TBM 层返回的结构化执行结果
 */
public class QueryResult {
    private ResultSet resultSet;
    private String message;
    private int affectedRows;
    private int resultRows;

    private QueryResult(ResultSet resultSet, String message, int affectedRows, int resultRows) {
        this.resultSet = resultSet;
        this.message = message;
        this.affectedRows = affectedRows;
        this.resultRows = resultRows;
    }

    public static QueryResult resultSet(ResultSet data) {
        int rows = data == null ? 0 : data.getRows().size();
        return new QueryResult(data, null, -1, rows);
    }

    public static QueryResult message(String message, int affectedRows) {
        return new QueryResult(null, message, affectedRows, -1);
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
