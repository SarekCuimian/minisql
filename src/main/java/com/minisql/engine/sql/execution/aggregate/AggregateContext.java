package com.minisql.engine.sql.execution.aggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.minisql.engine.sql.ast.expression.Aggregate;
import com.minisql.engine.table.Field;
import com.minisql.engine.table.FieldType;
import com.minisql.error.Error;
import com.minisql.result.ResultSet;

/**
 * 聚合器列表容器
 */
public class AggregateContext {
    private final List<Aggregator> aggregators;

    public AggregateContext(List<Aggregator> aggregators) {
        this.aggregators = aggregators;
    }

    public void accept(Map<String, Object> row) throws Exception {
        for (Aggregator agg : aggregators) {
            agg.accept(row);
        }
    }

    public List<String> labels() {
        List<String> headers = new ArrayList<>();
        for (Aggregator agg : aggregators) {
            headers.add(agg.label());
        }
        return headers;
    }

    public List<Object> values() {
        List<Object> values = new ArrayList<>();
        for (Aggregator agg : aggregators) {
            values.add(agg.value());
        }
        return values;
    }

    public List<String> stringValues() {
        List<String> values = new ArrayList<>();
        for (Aggregator agg : aggregators) {
            values.add(agg.stringValue());
        }
        return values;
    }

    public ResultSet toResultSet() {
        List<String> headers = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (Aggregator agg : aggregators) {
            headers.add(agg.label());
            values.add(agg.stringValue());
        }
        List<List<String>> rows = new ArrayList<>();
        rows.add(values);
        return new ResultSet(headers, rows);
    }

    public static AggregateContext  of(List<Field> fields, Aggregate[] aggregates) throws Exception {
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field f : fields) {
            fieldMap.put(f.getName(), f);
        }
        List<Aggregator> list = new ArrayList<>();
        // 遍历聚合数组，依次创建聚合器
        for (Aggregate agg : aggregates) {
            AggregateFunction function = agg.function;
            // 获取字段
            Field field = null;
            if(agg.field != null) {
                field = fieldMap.get(agg.field);
                if(field == null) {
                    throw Error.FieldNotFoundException;
                }
            }
            switch (function) {
                case COUNT:
                    list.add(new CountAggregator(field));
                    break;
                case SUM:
                    ensureNumeric(field);
                    list.add(new SumAggregator(field));
                    break;
                case AVG:
                    ensureNumeric(field);
                    list.add(new AvgAggregator(field));
                    break;
                case MIN:
                    if (field != null) {
                        list.add(new MinAggregator(field));
                    }
                    break;
                case MAX:
                    if (field != null) {
                        list.add(new MaxAggregator(field));
                    }
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        }
        return new AggregateContext(list);
    }

    private static void ensureNumeric(Field field) throws Exception {
        if(field == null) {
            throw Error.InvalidFieldException;
        }
        FieldType type = field.getType();
        if(type == FieldType.STRING) {
            throw Error.InvalidFieldException;
        }
    }
}
