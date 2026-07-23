package com.minisql.engine.storage.codec;

import java.util.Objects;

/**
 * byte array 连续区间的 zero-copy、有边界视图。
 *
 * <p>视图边界不可变；底层 bytes 保持可写，因此 Page layout 可在对应 Page latch
 * 保护下原地更新。</p>
 */
public final class ByteSlice {

    private final byte[] bytes;
    private final int offset;
    private final int length;

    public ByteSlice(byte[] bytes, int offset, int length) {
        this.bytes = Objects.requireNonNull(bytes, "bytes must not be null");
        Objects.checkFromIndexSize(offset, length, bytes.length);
        this.offset = offset;
        this.length = length;
    }

    public byte[] bytes() {
        return bytes;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public int end() {
        return offset + length;
    }

    /** 以当前 slice 为基准创建一个 zero-copy 子视图。 */
    public ByteSlice slice(int relativeOffset, int length) {
        Objects.checkFromIndexSize(relativeOffset, length, this.length);
        return new ByteSlice(bytes, offset + relativeOffset, length);
    }
}
