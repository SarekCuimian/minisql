package com.minisql.api.session;

import com.minisql.result.ExecutionResult;

public interface MiniSqlSession {
    ExecutionResult execute(String sql) throws Exception;
    void close();
}
