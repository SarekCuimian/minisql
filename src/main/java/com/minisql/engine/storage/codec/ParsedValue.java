package com.minisql.engine.storage.codec;

/**
 * 解析结果
 * 包含解析出的值与消费的字节数。
 */
public class ParsedValue {
    public final Object value;
    public final int size;

    public ParsedValue(Object value, int size) {
        this.value = value;
        this.size = size;
    }
}
