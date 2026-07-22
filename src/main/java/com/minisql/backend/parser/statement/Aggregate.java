package com.minisql.backend.parser.statement;

import com.minisql.backend.aggregator.AggregateFunction;

public class Aggregate {
    // count/sum/avg/min/max
    public AggregateFunction func;
    // null when count(*)
    public String field;      
}
