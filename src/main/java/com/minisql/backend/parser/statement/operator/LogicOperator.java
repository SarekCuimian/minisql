package com.minisql.backend.parser.statement.operator;

public enum LogicOperator {
    NONE,
    AND,
    OR;

    public String symbol() {
        return this == NONE ? "" : name().toLowerCase();
    }

    public static LogicOperator from(String op) {
        if (op == null || op.isEmpty()) {
            return NONE;
        }
        try {
            return LogicOperator.valueOf(op.toUpperCase());
        } catch (IllegalArgumentException ignore) {
            return NONE;
        }
    }
}
