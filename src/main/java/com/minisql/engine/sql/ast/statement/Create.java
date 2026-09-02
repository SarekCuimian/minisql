package com.minisql.engine.sql.ast.statement;

import com.minisql.engine.sql.ast.Statement;

public class Create implements Statement {
    public String tableName;
    public String[] fieldName;
    public String[] fieldType;
    public String[] index;
    public String[] unique;
    /**
     * 主键列名（仅允许单主键）
     */
    public String primary;
}
