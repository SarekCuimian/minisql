package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;

public class Show implements Statement {
    public enum Target {
        TABLES,
        DATABASES
    }

    public Target target = Target.TABLES;
}
