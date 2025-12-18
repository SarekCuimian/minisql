package com.minisql.api.entity.response;

/**
 * 通用 SQL 执行响应，getData 可为文本或结构化结果。
 */
public class SqlExecResponse<T> {

    private final boolean success;
    private final T data;
    private final String error;

    private SqlExecResponse(boolean success, T data, String error) {
        this.success = success;
        this.data = data;
        this.error = error;
    }

    public static <T> SqlExecResponse<T> success(T data) {
        return new SqlExecResponse<>(true, data, null);
    }

    public static <T> SqlExecResponse<T> failure(String message) {
        return new SqlExecResponse<>(false, null, message);
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
