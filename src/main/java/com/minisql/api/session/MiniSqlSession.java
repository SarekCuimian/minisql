package com.minisql.api.session;

import com.minisql.common.ExecResult;

public interface MiniSqlSession {
    ExecResult execute(String sql) throws Exception;
    void close();
}
