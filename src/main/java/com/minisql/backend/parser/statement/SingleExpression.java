package com.minisql.backend.parser.statement;

import com.minisql.backend.tbm.CompareOperator;

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
