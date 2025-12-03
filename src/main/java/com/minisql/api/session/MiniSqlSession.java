package com.minisql.api.session;

import com.minisql.backend.utils.format.ExecResult;

public interface MiniSqlSession {
    ExecResult execute(String sql) throws Exception;
    void close();
}
