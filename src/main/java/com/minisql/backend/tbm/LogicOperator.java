package com.minisql.backend.tbm;

public enum LogicOperator {
    NONE(""),
    AND("and"),
    OR("or");

    public final String symbol;

    LogicOperator(String symbol) {
        this.symbol = symbol;
    }

    public static LogicOperator from(String op) {
        if (op == null) return NONE;
        String lower = op.toLowerCase();
        for (LogicOperator lop : values()) {
            if (lop.symbol.equals(lower)) {
                return lop;
            }
        }
        return NONE;
    }
}
