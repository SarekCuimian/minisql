package com.minisql.engine.sql.ast.expression;

import com.minisql.engine.sql.ast.operator.CompareOperator;

/**
 * 基本谓词：left cop right
 */
public class PredicateCondition implements Condition {
    public Operand left;
    public CompareOperator cop;
    public Operand right;
}
