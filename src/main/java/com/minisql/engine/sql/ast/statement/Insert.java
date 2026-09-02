package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;

public class Insert implements Statement {
    public String tableName;
    public String[] columns;
    public String[] values;
}
