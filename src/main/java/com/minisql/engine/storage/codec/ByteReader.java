package com.minisql.engine.storage.codec;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * 在一个有界 byte array 区间内顺序读取基础值。
 *
 * <p>所有多字节值均使用 MiniSQL 磁盘格式规定的大端序。读取成功后游标自动前进；
 * 数据截断或内容不合法时抛出 {@link MalformedDataException}。</p>
 */
public final class ByteReader {

    private final byte[] bytes;
    private final int start;
    private final int limit;
    private int cursor;

    private ByteReader(byte[] bytes, int offset, int length) {
        this.bytes = Objects.requireNonNull(bytes, "bytes must not be null");
        Objects.checkFromIndexSize(offset, length, bytes.length);
        this.start = offset;
        this.cursor = offset;
        this.limit = offset + length;
    }

    public static ByteReader wrap(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        return new ByteReader(bytes, 0, bytes.length);
    }

    public static ByteReader wrap(byte[] bytes, int offset, int length) {
        return new ByteReader(bytes, offset, length);
    }

    public static ByteReader wrap(ByteSlice slice) {
        Objects.requireNonNull(slice, "slice must not be null");
        return new ByteReader(slice.bytes(), slice.offset(), slice.length());
    }

    /** 返回相对于当前读取区间起点的位置。 */
    public int position() {
        return cursor - start;
    }

    public int remaining() {
        return limit - cursor;
    }

    public boolean hasRemaining() {
        return cursor < limit;
    }

    public byte readByte() {
        requireRemaining(Byte.BYTES);
        return bytes[cursor++];
    }

    public int readUnsignedByte() {
        return Byte.toUnsignedInt(readByte());
    }

    /**
     * 读取严格的 boolean 编码：{@code 0} 表示 false，{@code 1} 表示 true。
     */
    public boolean readBoolean() {
        int valuePosition = position();
        int value = readUnsignedByte();
        if (value == 0) {
            return false;
        }
        if (value == 1) {
            return true;
        }
        throw new MalformedDataException(
                "Invalid boolean value " + value + " at position " + valuePosition
        );
    }

    public short readShort() {
        requireRemaining(Short.BYTES);
        short value = ByteUtil.getShort(bytes, cursor);
        cursor += Short.BYTES;
        return value;
    }

    public int readUnsignedShort() {
        return Short.toUnsignedInt(readShort());
    }

    public int readInt() {
        requireRemaining(Integer.BYTES);
        int value = ByteUtil.getInt(bytes, cursor);
        cursor += Integer.BYTES;
        return value;
    }

    public long readLong() {
        requireRemaining(Long.BYTES);
        long value = ByteUtil.getLong(bytes, cursor);
        cursor += Long.BYTES;
        return value;
    }

    /** 读取并复制指定长度的字节。 */
    public byte[] readBytes(int length) {
        requireRemaining(length);
        byte[] value = Arrays.copyOfRange(bytes, cursor, cursor + length);
        cursor += length;
        return value;
    }

    /** 读取一个共享底层数组的 zero-copy 字节视图。 */
    public ByteSlice readSlice(int length) {
        requireRemaining(length);
        ByteSlice value = new ByteSlice(bytes, cursor, length);
        cursor += length;
        return value;
    }

    /** 读取指定字节长度的严格 UTF-8 字符串。 */
    public String readUtf8(int byteLength) {
        requireRemaining(byteLength);
        int valuePosition = position();
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes, cursor, byteLength))
                    .toString();
            cursor += byteLength;
            return value;
        } catch (CharacterCodingException e) {
            throw new MalformedDataException(
                    "Invalid UTF-8 value at position " + valuePosition
                            + " with byte length " + byteLength,
                    e
            );
        }
    }

    public void skip(int length) {
        requireRemaining(length);
        cursor += length;
    }

    /**
     * 要求当前读取区间已被完整消费，防止格式解析静默忽略尾部数据。
     */
    public void requireFullyConsumed() {
        int remaining = remaining();
        if (remaining != 0) {
            throw new MalformedDataException(
                    "Binary format contains " + remaining
                            + " trailing bytes at position " + position()
            );
        }
    }

    private void requireRemaining(int required) {
        if (required < 0) {
            throw new MalformedDataException(
                    "Required byte count must not be negative: " + required
            );
        }
        if (required > remaining()) {
            throw new MalformedDataException(
                    "Cannot read " + required + " bytes at position " + position()
                            + ": only " + remaining() + " bytes remain"
            );
        }
    }
}
