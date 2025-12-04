package com.minisql.api.entity.enums;

/**
 * SQL 返回格式：文本或结构化。
 */
public enum ResponseFormat {
    TEXT,
    STRUCTURED;

    public boolean isText() {
        return this == TEXT;
    }

    /**
     * 解析客户端传入的字符串，默认 STRUCTURED
     */
    public static ResponseFormat from(String value) {
        if(value == null || value.isBlank()) {
            return STRUCTURED;
        }
        try {
            return ResponseFormat.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return STRUCTURED;
        }
    }
}
