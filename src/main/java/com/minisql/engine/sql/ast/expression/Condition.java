package com.minisql.engine.sql.ast.expression;

import com.minisql.engine.sql.ast.operator.CompareOperator;
import com.minisql.engine.sql.ast.operator.LogicOperator;

/**
 * HAVING 条件节点标记接口
 */
public interface Condition {

    static PredicateCondition ofPredicate(Operand left, CompareOperator cop, Operand right) {
        PredicateCondition condition = new PredicateCondition();
        condition.left = left;
        condition.cop = cop;
        condition.right = right;
        return condition;
    }

    static BinaryCondition ofBinary(Condition left, LogicOperator lop, Condition right) {
        BinaryCondition condition = new BinaryCondition();
        condition.left = left;
        condition.lop = lop;
        condition.right = right;
        return condition;
    }
}
