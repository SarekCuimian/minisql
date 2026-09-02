package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;

/**
 * CREATE DATABASE 语句
 */
public class CreateDatabase implements Statement {
    public String databaseName;
}
