package com.minisql.common;

/**
 * TBM 层返回的结构化执行结果，不包含任何格式化后的文本。
 */
public class OpResult {
    private ResultSet resultSet;
    private String message;
    private int affectedRows;
    private int resultRows;

    // 默认构造函数供序列化框架使用
    public OpResult() {}

    private OpResult(ResultSet resultSet, String message, int affectedRows, int resultRows) {
        this.resultSet = resultSet;
        this.message = message;
        this.affectedRows = affectedRows;
        this.resultRows = resultRows;
    }

    public static OpResult resultSet(ResultSet data) {
        int rows = data == null ? 0 : data.getRows().size();
        return new OpResult(data, null, -1, rows);
    }

    public static OpResult message(String message, int affectedRows) {
        return new OpResult(null, message, affectedRows, -1);
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
