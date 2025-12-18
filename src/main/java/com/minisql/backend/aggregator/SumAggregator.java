package com.minisql.backend.aggregator;

import java.util.Map;

import com.minisql.backend.tbm.Field;

public class SumAggregator implements Aggregator {
    private final Field field;
    private final String label;
    private long sum = 0;

    public SumAggregator(Field field) {
        this.field = field;
        this.label = "SUM(" + field.getName() + ")";
    }

    @Override
    public void accept(Map<String, Object> row) {
        Object v = row.get(field.getName());
        if(v != null) {
            sum += ((Number) v).longValue();
        }
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Object value() {
        return sum;
    }

    @Override
    public String stringValue() {
        return String.valueOf(sum);
    }
}
