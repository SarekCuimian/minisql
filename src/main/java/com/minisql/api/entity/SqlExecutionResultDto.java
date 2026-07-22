package com.minisql.api.entity;

import com.minisql.common.ExecutionResult;
import com.minisql.common.ResultSet;

import java.util.Collections;
import java.util.List;

/**
 * SQL 执行结果的 API 数据传输对象。
 */
public class SqlExecutionResultDto {
    private final ExecutionResult.Type type;
    private final List<String> headers;
    private final List<List<String>> rows;
    private final String message;
    private final long elapsedNanos;
    private final int affectedRows;
    private final int resultRows;

    private SqlExecutionResultDto(ExecutionResult.Type type,
                                  long elapsedNanos,
                                  List<String> headers,
                                  List<List<String>> rows,
                                  String message,
                                  int affectedRows,
                                  int resultRows) {
        this.type = type;
        this.elapsedNanos = elapsedNanos;
        this.headers = headers;
        this.rows = rows;
        this.message = message;
        this.affectedRows = affectedRows;
        this.resultRows = resultRows;
    }

    public static SqlExecutionResultDto from(ExecutionResult executionResult) {
        if (executionResult == null) {
            throw new IllegalArgumentException("executionResult must not be null");
        }
        ResultSet resultSet = executionResult.getStatementResult() == null
                ? null
                : executionResult.getStatementResult().getResultSet();
        return new SqlExecutionResultDto(
                executionResult.getType(),
                executionResult.getElapsedNanos(),
                resultSet == null ? Collections.emptyList() : resultSet.getHeaders(),
                resultSet == null ? Collections.emptyList() : resultSet.getRows(),
                executionResult.getStatementResult() == null ? null : executionResult.getStatementResult().getMessage(),
                executionResult.getStatementResult() == null ? -1 : executionResult.getStatementResult().getAffectedRows(),
                executionResult.getStatementResult() == null ? -1 : executionResult.getStatementResult().getResultRows()
        );
    }

    public ExecutionResult.Type getType() {
        return type;
    }

    public long getElapsedNanos() {
        return elapsedNanos;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<String>> getRows() {
        return rows;
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
