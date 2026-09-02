package com.minisql.engine.sql.ast.expression;

import com.minisql.engine.sql.ast.operator.LogicOperator;

/**
 * 逻辑组合条件：AND / OR
 */
public class BinaryCondition implements Condition {
    public Condition left;
    public LogicOperator lop;
    public Condition right;
}
