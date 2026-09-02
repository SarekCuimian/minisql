package com.minisql.engine.sql.ast.expression;

import com.minisql.engine.sql.ast.operator.CompareOperator;

public class SingleExpression {
    public final String field;
    public final CompareOperator op;
    public final String value;

    public SingleExpression(String field, CompareOperator op, String value) {
        this.field = field;
        this.op = op;
        this.value = value;
    }
}
