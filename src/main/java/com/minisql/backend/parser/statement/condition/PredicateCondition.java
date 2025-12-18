package com.minisql.backend.parser.statement.condition;

import com.minisql.backend.parser.statement.operator.CompareOperator;

/**
 * 基本谓词：left cop right
 */
public class PredicateCondition implements Condition {
    public Operand left;
    public CompareOperator cop;
    public Operand right;
}
