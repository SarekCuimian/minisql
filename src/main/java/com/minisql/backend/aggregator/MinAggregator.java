package com.minisql.backend.aggregator;

import java.util.Map;

import com.minisql.backend.tbm.Field;

public class MinAggregator implements Aggregator {
    private final Field field;
    private final String label;
    private Object min;

    public MinAggregator(Field field) {
        this.field = field;
        this.label = "MIN(" + field.getName() + ")";
    }

    @Override
    public void accept(Map<String, Object> row) throws Exception {
        Object v = row.get(field.getName());
        if(v == null) {
            return;
        }
        if(min == null || field.getType().compare(v, min) < 0) {
            min = v;
        }
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Object value() {
        return min;
    }

    @Override
    public String stringValue() {
        return min == null ? "NULL" : field.stringValue(min);
    }
}
