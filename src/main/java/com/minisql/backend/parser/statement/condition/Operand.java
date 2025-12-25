package com.minisql.backend.parser.statement.condition;

/**
 * HAVING 条件中的操作数。
 */
public class Operand {
    public enum Kind {
        COLUMN,
        AGGREGATE,
        CONSTANT
    }

    public Kind kind;
    public int groupIndex = -1;     // groupBy 中的下标
    public int aggregateIndex = -1; // 对应 select.aggregates 的下标
    public Object constant;         // 常量值

    public static Operand ofColumn(int index) {
        Operand operand = new Operand();
        operand.kind = Kind.COLUMN;
        operand.groupIndex = index;
        return operand;
    }

    public static Operand ofAggregate(int index) {
        Operand operand = new Operand();
        operand.kind = Kind.AGGREGATE;
        operand.aggregateIndex = index;
        return operand;
    }

    public static Operand ofConstant(Object constant) {
        Operand operand = new Operand();
        operand.kind = Kind.CONSTANT;
        operand.constant = constant;
        return operand;
    }
}
