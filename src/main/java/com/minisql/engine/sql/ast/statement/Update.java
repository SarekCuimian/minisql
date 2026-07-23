package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;
import com.minisql.engine.sql.ast.clause.Where;

public class Update implements Statement {
    public String tableName;
    public String fieldName;
    public String value;
    public Where where;
}
