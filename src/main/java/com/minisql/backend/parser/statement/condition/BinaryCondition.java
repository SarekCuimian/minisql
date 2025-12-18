package com.minisql.backend.parser.statement.condition;

import com.minisql.backend.parser.statement.operator.LogicOperator;

/**
 * 逻辑组合条件：AND / OR
 */
public class BinaryCondition implements Condition {
    public Condition left;
    public LogicOperator lop;
    public Condition right;
}
