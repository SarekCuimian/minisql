package com.minisql.engine.sql.ast.expression;

import com.minisql.engine.sql.execution.aggregate.AggregateFunction;

public class Aggregate {
    // count/sum/avg/min/max
    public AggregateFunction function;
    // null when count(*)
    public String field;      
}
