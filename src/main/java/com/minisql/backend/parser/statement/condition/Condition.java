package com.minisql.backend.parser.statement.condition;

import com.minisql.backend.parser.statement.operator.CompareOperator;
import com.minisql.backend.parser.statement.operator.LogicOperator;

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
