package com.minisql.common;

import java.util.Collections;
import java.util.List;

/**
 * 结构化的查询结果数据，包含列头和行值。
 * 值使用字符串表示，具体格式由客户端自行渲染。
 */
public class ResultSet {
    private final List<String> headers;
    private final List<List<String>> rows;

    public ResultSet(List<String> headers, List<List<String>> rows) {
        this.headers = headers == null ? Collections.emptyList() : headers;
        this.rows = rows == null ? Collections.emptyList() : rows;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<String>> getRows() {
        return rows;
    }
}
