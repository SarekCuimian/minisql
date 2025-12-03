package com.minisql.api.dto;

public class SqlExecResponse {

    private final boolean success;
    private final String data;
    private final String error;

    private SqlExecResponse(boolean success, String data, String error) {
        this.success = success;
        this.data = data;
        this.error = error;
    }

    public static SqlExecResponse success(String data) {
        return new SqlExecResponse(true, data, null);
    }

    public static SqlExecResponse failure(String message) {
        return new SqlExecResponse(false, null, message);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getData() {
        return data;
    }

    public String getError() {
        return error;
    }
}
