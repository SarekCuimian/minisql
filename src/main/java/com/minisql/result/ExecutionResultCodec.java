package com.minisql.result;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.nio.charset.StandardCharsets;

/**
 * 负责在网络上传输 {@link ExecutionResult} 的编解码器。
 */
public final class ExecutionResultCodec {

    private static final Gson GSON = new Gson();

    private ExecutionResultCodec() {
    }

    public static byte[] encode(ExecutionResult result) {
        Content content = Content.from(result);
        return GSON.toJson(content).getBytes(StandardCharsets.UTF_8);
    }

    public static ExecutionResult decode(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("ExecutionResult payload is empty");
        }
        Content content = GSON.fromJson(new String(data, StandardCharsets.UTF_8), Content.class);
        if (content == null) {
            throw new IllegalArgumentException("Unable to decode ExecutionResult");
        }
        return content.toResult();
    }

    private static class Content {
        ExecutionResult.Type type;
        @SerializedName("queryResult")
        StatementResult statementResult;
        long elapsedNanos;

        static Content from(ExecutionResult result) {
            Content content = new Content();
            content.type = result.getType();
            content.statementResult = result.getStatementResult();
            content.elapsedNanos = result.getElapsedNanos();
            return content;
        }

        ExecutionResult toResult() {
            return ExecutionResult.from(statementResult, type, elapsedNanos);
        }
    }
}
