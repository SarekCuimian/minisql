package com.minisql.backend.aggregator;

import java.util.Map;

/**
 * 聚合器接口，封装单个聚合函数的状态与运算。
 */
public interface Aggregator {
    /** 每行数据调用一次 */
    void accept(Map<String, Object> row) throws Exception;

    /** 聚合列名，如 COUNT(*)、SUM(age) */
    String label();

    /** 聚合结果的原始值（用于后续计算，例如 HAVING） */
    Object value();

    /** 聚合结果的字符串表示（用于输出） */
    String stringValue();
}
