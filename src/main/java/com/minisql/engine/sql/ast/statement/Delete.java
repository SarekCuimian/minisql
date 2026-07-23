package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;
import com.minisql.engine.sql.ast.clause.Where;

public class Delete implements Statement {
    public String tableName;
    public Where where;
}
