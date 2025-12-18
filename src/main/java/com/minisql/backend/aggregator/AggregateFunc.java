package com.minisql.backend.aggregator;

import java.util.Locale;

public enum AggregateFunc {
    COUNT(true),
    SUM(false),
    AVG(false),
    MIN(false),
    MAX(false);

    private final boolean allowStar;

    AggregateFunc(boolean allowStar) {
        this.allowStar = allowStar;
    }

    public boolean allowStar() {
        return allowStar;
    }

    public static AggregateFunc from(String s) {
        return AggregateFunc.valueOf(s.toUpperCase(Locale.ROOT));
    }
}
