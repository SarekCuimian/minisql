package com.minisql.backend.aggregator;

import java.util.Map;

import com.minisql.backend.tbm.Field;

public class AvgAggregator implements Aggregator {
    private final Field field;
    private final String label;
    private long sum = 0;
    private long count = 0;

    public AvgAggregator(Field field) {
        this.field = field;
        this.label = "AVG(" + field.getName() + ")";
    }

    @Override
    public void accept(Map<String, Object> row) {
        Object v = row.get(field.getName());
        if(v != null) {
            sum += ((Number) v).longValue();
            count++;
        }
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Object value() {
        if(count == 0) {
            return 0d;
        }
        return (double) sum / count;
    }

    @Override
    public String stringValue() {
        return String.valueOf(value());
    }
}
