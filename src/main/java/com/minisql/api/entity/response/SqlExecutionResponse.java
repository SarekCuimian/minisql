package com.minisql.api.entity.response;

/**
 * 通用 SQL 执行响应，data 可为文本或结构化结果。
 */
public class SqlExecutionResponse<T> {

    private final boolean success;
    private final T data;
    private final String error;

    private SqlExecutionResponse(boolean success, T data, String error) {
        this.success = success;
        this.data = data;
        this.error = error;
    }

    public static <T> SqlExecutionResponse<T> success(T data) {
        return new SqlExecutionResponse<>(true, data, null);
    }

    public static <T> SqlExecutionResponse<T> failure(String message) {
        return new SqlExecutionResponse<>(false, null, message);
    }

    public boolean isSuccess() {
        return success;
    }

    public T getData() {
        return data;
    }

    public String getError() {
        return error;
    }
}
