package com.minisql.backend.parser.statement;

import com.minisql.backend.parser.statement.condition.Condition;

public class Select {
    public String tableName;
    public String[] fields;
    public Aggregate[] aggregates;
    public Item[] projection;
    public Where where;
    public String[] groupBy;
    public Condition having;

    public static class Item {
        public enum Kind {
            COLUMN,
            AGGREGATE
        }

        public Kind kind;
        public String field;        // kind=COLUMN 时使用
        public Aggregate aggregate; // kind=AGGREGATE 时使用
        public String alias;        // 可选的输出别名

        public boolean isAggregate() {
            return kind == Kind.AGGREGATE;
        }

        public static Item ofColumn(String name, String alias) {
            Item item = new Item();
            item.kind = Kind.COLUMN;
            item.field = name;
            item.alias = alias != null ? alias : name;
            return item;
        }

        public static Item ofColumn(String name) {
            return ofColumn(name, null);
        }

        public static Item ofAggregate(Aggregate agg, String alias) {
            Item item = new Item();
            item.kind = Kind.AGGREGATE;
            item.aggregate = agg;
            item.alias = alias;
            return item;
        }
    }
}
