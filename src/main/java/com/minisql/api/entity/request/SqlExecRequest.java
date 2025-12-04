package com.minisql.api.entity.request;

import com.minisql.api.entity.enums.ResponseFormat;

import javax.validation.constraints.NotBlank;

public class SqlExecRequest {

    @NotBlank(message = "sql 不能为空")
    private String sql;

    /**
     * 返回格式：TEXT（默认）或 STRUCTURED
     */
    private ResponseFormat format;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public ResponseFormat getFormat() {
        return format;
    }

    public void setFormat(ResponseFormat format) {
        this.format = format;
    }
}
