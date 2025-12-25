package com.minisql.backend.tbm;

import com.minisql.common.Error;

public enum FieldType {
    STRING {
        @Override
        public Object parse(String raw) {
            return raw;
        }

        @Override
        public int compare(Object left, Object right) {
            return ((String) left).compareTo((String) right);
        }
    },

    INT32 {
        @Override
        public Object parse(String raw) {
            return Integer.valueOf(raw);
        }

        @Override
        public int compare(Object left, Object right) {
            long l = ((Number) left).longValue();
            long r = ((Number) right).longValue();
            return Long.compare(l, r);
        }
    },

    INT64 {
        @Override
        public Object parse(String raw) {
            return Long.valueOf(raw);
        }

        @Override
        public int compare(Object left, Object right) {
            long l = ((Number) left).longValue();
            long r = ((Number) right).longValue();
            return Long.compare(l, r);
        }
    };

    public abstract Object parse(String raw);

    public abstract int compare(Object left, Object right);

    public static FieldType from(String s) throws Exception {
        switch (s) {
            case "string":
                return STRING;
            case "int32":
                return INT32;
            case "int64":
                return INT64;
            default:
                throw Error.InvalidFieldException;
        }
    }
}
