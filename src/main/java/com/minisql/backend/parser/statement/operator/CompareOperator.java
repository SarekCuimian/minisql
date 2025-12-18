package com.minisql.backend.parser.statement.operator;

import com.minisql.common.Error;

public enum CompareOperator {
    LT("<") {
        @Override
        public boolean match(int cmp) {
            return cmp < 0;
        }
    },
    LE("<=") {
        @Override
        public boolean match(int cmp) {
            return cmp <= 0;
        }
    },
    EQ("=") {
        @Override
        public boolean match(int cmp) {
            return cmp == 0;
        }
    },
    GT(">") {
        @Override
        public boolean match(int cmp) {
            return cmp > 0;
        }
    },
    GE(">=") {
        @Override
        public boolean match(int cmp) {
            return cmp >= 0;
        }
    },
    NE("!=", "<>") {
        @Override
        public boolean match(int cmp) {
            return cmp != 0;
        }
    };

    public final String[] symbols;

    CompareOperator(String... symbols) {
        this.symbols = symbols;
    }

    public abstract boolean match(int cmp);

    public static CompareOperator from(String symbol) throws Exception {
        if (symbol == null) {
            throw Error.InvalidLogOpException;
        }
        for (CompareOperator op : values()) {
            for (String s : op.symbols) {
                if (s.equals(symbol)) {
                    return op;
                }
            }
        }
        throw Error.InvalidLogOpException;
    }
}
