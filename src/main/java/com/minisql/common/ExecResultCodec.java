package com.minisql.common;

import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;

/**
 * Codec 编解码器
 * 负责在网络上传输 {@link ExecResult} 的编解码工具。
 */
public final class ExecResultCodec {

    private static final Gson GSON = new Gson();

    private ExecResultCodec() {
    }

    public static byte[] encode(ExecResult result) {
        Content content = Content.from(result);
        return GSON.toJson(content).getBytes(StandardCharsets.UTF_8);
    }

    public static ExecResult decode(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("ExecResult payload is empty");
        }
        Content content = GSON.fromJson(new String(data, StandardCharsets.UTF_8), Content.class);
        if (content == null) {
            throw new IllegalArgumentException("Unable to decode ExecResult");
        }
        return content.toResult();
    }

    private static class Content {
        ExecResult.Type type;
        OpResult opResult;
        long elapsedNanos;

        static Content from(ExecResult result) {
            Content content = new Content();
            content.type = result.getType();
            content.opResult = result.getOpResult();
            content.elapsedNanos = result.getElapsedNanos();
            return content;
        }

        ExecResult toResult() {
            return ExecResult.from(opResult, type, elapsedNanos);
        }
    }
}
