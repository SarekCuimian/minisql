package com.minisql.backend.aggregator;

import java.util.Map;

import com.minisql.backend.tbm.Field;

public class MaxAggregator implements Aggregator {
    private final Field field;
    private final String label;
    private Object max;

    public MaxAggregator(Field field) {
        this.field = field;
        this.label = "MAX(" + field.getName() + ")";
    }

    @Override
    public void accept(Map<String, Object> row) throws Exception {
        Object v = row.get(field.getName());
        if(v == null) {
            return;
        }
        if(max == null || field.getType().compare(v, max) > 0) {
            max = v;
        }
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Object value() {
        return max;
    }

    @Override
    public String stringValue() {
        return max == null ? "NULL" : field.stringValue(max);
    }
}
