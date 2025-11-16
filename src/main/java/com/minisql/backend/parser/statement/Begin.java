package com.minisql.backend.parser.statement;

import com.minisql.backend.vm.IsolationLevel;

public class Begin {
    /**
     * 事务隔离级别，默认 READ COMMITTED
     */
    public IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
}
