package com.minisql.engine.storage.codec;

import java.nio.ByteOrder;
import java.util.Objects;

/**
 * 磁盘格式使用的 big-endian binary codec。
 *
 * <p>所有 primitive 操作都直接定位既有 byte array，因此解析 Page 时不会创建
 * 临时数组或 {@code ByteBuffer}。</p>
 */
public final class ByteUtil {

    /** MiniSQL 所有持久化 binary format 统一使用的字节序。 */
    public static final ByteOrder DISK_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private ByteUtil() {
    }

    public static short getShort(byte[] bytes, int offset) {
        checkRange(bytes, offset, Short.BYTES);
        return (short) (((bytes[offset] & 0xFF) << 8)
                | (bytes[offset + 1] & 0xFF));
    }

    public static void putShort(byte[] bytes, int offset, short value) {
        checkRange(bytes, offset, Short.BYTES);
        bytes[offset] = (byte) (value >>> 8);
        bytes[offset + 1] = (byte) value;
    }

    public static int getInt(byte[] bytes, int offset) {
        checkRange(bytes, offset, Integer.BYTES);
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    public static void putInt(byte[] bytes, int offset, int value) {
        checkRange(bytes, offset, Integer.BYTES);
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    public static long getLong(byte[] bytes, int offset) {
        checkRange(bytes, offset, Long.BYTES);
        return ((long) (bytes[offset] & 0xFF) << 56)
                | ((long) (bytes[offset + 1] & 0xFF) << 48)
                | ((long) (bytes[offset + 2] & 0xFF) << 40)
                | ((long) (bytes[offset + 3] & 0xFF) << 32)
                | ((long) (bytes[offset + 4] & 0xFF) << 24)
                | ((long) (bytes[offset + 5] & 0xFF) << 16)
                | ((long) (bytes[offset + 6] & 0xFF) << 8)
                | ((long) (bytes[offset + 7] & 0xFF));
    }

    public static void putLong(byte[] bytes, int offset, long value) {
        checkRange(bytes, offset, Long.BYTES);
        bytes[offset] = (byte) (value >>> 56);
        bytes[offset + 1] = (byte) (value >>> 48);
        bytes[offset + 2] = (byte) (value >>> 40);
        bytes[offset + 3] = (byte) (value >>> 32);
        bytes[offset + 4] = (byte) (value >>> 24);
        bytes[offset + 5] = (byte) (value >>> 16);
        bytes[offset + 6] = (byte) (value >>> 8);
        bytes[offset + 7] = (byte) value;
    }

    private static void checkRange(byte[] bytes, int offset, int length) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        Objects.checkFromIndexSize(offset, length, bytes.length);
    }
}
