package com.minisql.web.session;

import com.minisql.backend.utils.format.ExecResult;

public interface MiniSqlSession {
    ExecResult execute(String sql) throws Exception;
    void close();
}
