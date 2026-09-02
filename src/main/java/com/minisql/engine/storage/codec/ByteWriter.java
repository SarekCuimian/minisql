package com.minisql.engine.storage.codec;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 向一个定长 byte array 顺序写入基础值。
 *
 * <p>所有多字节值均使用 MiniSQL 磁盘格式规定的大端序。Writer 不自动扩容，
 * 以便尽早发现编码长度计算错误。</p>
 */
public final class ByteWriter {

    private final byte[] bytes;
    private int cursor;

    private ByteWriter(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity must not be negative: " + capacity);
        }
        this.bytes = new byte[capacity];
    }

    public static ByteWriter allocate(int capacity) {
        return new ByteWriter(capacity);
    }

    public int position() {
        return cursor;
    }

    public int remaining() {
        return bytes.length - cursor;
    }

    public boolean hasRemaining() {
        return cursor < bytes.length;
    }

    public void writeByte(byte value) {
        requireCapacity(Byte.BYTES);
        bytes[cursor++] = value;
    }

    public void writeBoolean(boolean value) {
        writeByte((byte) (value ? 1 : 0));
    }

    public void writeShort(short value) {
        requireCapacity(Short.BYTES);
        ByteUtil.putShort(bytes, cursor, value);
        cursor += Short.BYTES;
    }

    public void writeInt(int value) {
        requireCapacity(Integer.BYTES);
        ByteUtil.putInt(bytes, cursor, value);
        cursor += Integer.BYTES;
    }

    public void writeLong(long value) {
        requireCapacity(Long.BYTES);
        ByteUtil.putLong(bytes, cursor, value);
        cursor += Long.BYTES;
    }

    public void writeBytes(byte[] value) {
        Objects.requireNonNull(value, "value must not be null");
        writeBytes(value, 0, value.length);
    }

    public void writeBytes(byte[] value, int offset, int length) {
        Objects.requireNonNull(value, "value must not be null");
        Objects.checkFromIndexSize(offset, length, value.length);
        requireCapacity(length);
        System.arraycopy(value, offset, bytes, cursor, length);
        cursor += length;
    }

    public void writeSlice(ByteSlice value) {
        Objects.requireNonNull(value, "value must not be null");
        writeBytes(value.bytes(), value.offset(), value.length());
    }

    /** 将字符串编码为 UTF-8 原始字节，不自动写入长度字段。 */
    public void writeUtf8(String value) {
        Objects.requireNonNull(value, "value must not be null");
        writeBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 返回完整编码结果。调用前必须恰好写满分配容量。
     */
    public byte[] toByteArray() {
        requireFullyWritten();
        return bytes;
    }

    public void requireFullyWritten() {
        int remaining = remaining();
        if (remaining != 0) {
            throw new IllegalStateException(
                    "Binary format is incomplete: " + remaining
                            + " unwritten bytes remain at position " + position()
            );
        }
    }

    private void requireCapacity(int required) {
        if (required > remaining()) {
            throw new IllegalStateException(
                    "Cannot write " + required + " bytes at position " + position()
                            + ": only " + remaining() + " bytes remain"
            );
        }
    }
}
