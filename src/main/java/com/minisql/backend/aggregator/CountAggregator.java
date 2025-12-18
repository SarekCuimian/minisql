package com.minisql.backend.aggregator;

import java.util.Map;

import com.minisql.backend.tbm.Field;

public class CountAggregator implements Aggregator {
    private final Field field; // null for count(*)
    private final String label;
    private long count = 0;

    public CountAggregator(Field field) {
        this.field = field;
        this.label =  (field == null) ? "COUNT(*)" : "COUNT(" + field.getName() + ")";
    }

    @Override
    public void accept(Map<String, Object> row) {
        if(field == null) {
            count++;
        } else {
            Object v = row.get(field.getName());
            if(v != null) {
                count++;
            }
        }
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Object value() {
        return count;
    }

    @Override
    public String stringValue() {
        return String.valueOf(count);
    }
}
