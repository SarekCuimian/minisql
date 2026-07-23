package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;

/**
 * USE database 语句
 */
public class Use implements Statement {
    public String databaseName;
}
