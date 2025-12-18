package com.minisql.backend.parser.statement;

import com.minisql.backend.aggregator.AggregateFunc;

public class Aggregate {
    // count/sum/avg/min/max
    public AggregateFunc func; 
    // null when count(*)
    public String field;      
}
