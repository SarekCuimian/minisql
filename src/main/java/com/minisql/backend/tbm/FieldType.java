package com.minisql.backend.tbm;

import com.minisql.common.Error;

public enum FieldType {
    STRING {
        @Override
        public Object parse(String str) {
            return str;
        }

        @Override
        public int compare(Object left, Object right) {
            return ((String) left).compareTo((String) right);
        }

        @Override
        protected Object defaultValue() {
            return "";
        }
    },

    INT32 {
        @Override
        public Object parse(String str) {
            return Integer.valueOf(str);
        }

        @Override
        public int compare(Object left, Object right) {
            long l = ((Number) left).longValue();
            long r = ((Number) right).longValue();
            return Long.compare(l, r);
        }

        @Override
        protected Object defaultValue() {
            return 0;
        }
    },

    INT64 {
        @Override
        public Object parse(String str) {
            return Long.valueOf(str);
        }

        @Override
        public int compare(Object left, Object right) {
            long l = ((Number) left).longValue();
            long r = ((Number) right).longValue();
            return Long.compare(l, r);
        }

        @Override
        protected Object defaultValue() {
            return 0L;
        }
    };

    public abstract Object parse(String str);

    public abstract int compare(Object left, Object right);

    protected abstract Object defaultValue();

    /**
     * 从字段类型字符串解析 FieldType
     */
    public static FieldType from(String s) throws Exception {
        if (s == null) {
            throw Error.InvalidFieldException;
        }
        try {
            // "string" -> "STRING" -> FieldType.STRING
            return FieldType.valueOf(s.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw Error.InvalidFieldException;
        }
    }

}

