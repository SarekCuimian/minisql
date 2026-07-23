package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;

/**
 * DROP DATABASE 语句
 */
public class DropDatabase implements Statement {
    public String databaseName;
}
