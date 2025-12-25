package com.minisql.api.entity;

import com.minisql.common.ExecResult;
import com.minisql.common.ResultSet;

import java.util.Collections;
import java.util.List;

/**
 * SQL 执行结果
 */
public class SqlExecResult {
    private final ExecResult.Type type;
    private final List<String> headers;
    private final List<List<String>> rows;
    private final String message;
    private final long elapsedNanos;
    private final int affectedRows;
    private final int resultRows;

    private SqlExecResult(ExecResult.Type type,
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

    public static SqlExecResult from(ExecResult execResult) {
        if (execResult == null) {
            throw new IllegalArgumentException("execResult must not be null");
        }
        ResultSet rs = execResult.getOpResult() == null ? null : execResult.getOpResult().getResultSet();
        return new SqlExecResult(
                execResult.getType(),
                execResult.getElapsedNanos(),
                rs == null ? Collections.emptyList() : rs.getHeaders(),
                rs == null ? Collections.emptyList() : rs.getRows(),
                execResult.getOpResult() == null ? null : execResult.getOpResult().getMessage(),
                execResult.getOpResult() == null ? -1 : execResult.getOpResult().getAffectedRows(),
                execResult.getOpResult() == null ? -1 : execResult.getOpResult().getResultRows()
        );
    }

    public ExecResult.Type getType() {
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
