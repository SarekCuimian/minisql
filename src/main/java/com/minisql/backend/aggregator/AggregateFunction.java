package com.minisql.backend.aggregator;

import java.util.Locale;

public enum AggregateFunction {
    COUNT(true),
    SUM(false),
    AVG(false),
    MIN(false),
    MAX(false);

    private final boolean allowStar;

    AggregateFunction(boolean allowStar) {
        this.allowStar = allowStar;
    }

    public boolean allowStar() {
        return allowStar;
    }

    public static AggregateFunction from(String value) {
        return AggregateFunction.valueOf(value.toUpperCase(Locale.ROOT));
    }
}
