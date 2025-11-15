package top.guoziyang.mydb.backend.utils.format;

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
        Message message = Message.from(result);
        return GSON.toJson(message).getBytes(StandardCharsets.UTF_8);
    }

    public static ExecResult decode(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("ExecResult payload is empty");
        }
        Message message = GSON.fromJson(new String(data, StandardCharsets.UTF_8), Message.class);
        if (message == null) {
            throw new IllegalArgumentException("Unable to decode ExecResult");
        }
        return message.toResult();
    }

    private static class Message {
        ExecResult.Type type;
        byte[] payload;
        long elapsedNanos;
        int resultRows;
        int affectedRows;

        static Message from(ExecResult result) {
            Message message = new Message();
            message.type = result.getType();
            message.payload = result.getPayload();
            message.elapsedNanos = result.getElapsedNanos();
            message.resultRows = result.getResultRows();
            message.affectedRows = result.getAffectedRows();
            return message;
        }

        ExecResult toResult() {
            if (type == ExecResult.Type.RESULT) {
                return ExecResult.resultSet(payload, elapsedNanos, resultRows);
            }
            return ExecResult.okPacket(payload, elapsedNanos, affectedRows);
        }
    }
}
