package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.transaction.mvcc.IsolationLevel;
import com.minisql.engine.sql.ast.Statement;

public class Begin implements Statement {
    /**
     * 事务隔离级别，默认 READ COMMITTED
     */
    public IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
}
