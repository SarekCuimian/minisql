package com.minisql.engine.storage.codec;

/**
 * 输入数据不符合预期的编码结构。
 */
public final class MalformedDataException extends RuntimeException {

    public MalformedDataException(String message) {
        super(message);
    }

    public MalformedDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
