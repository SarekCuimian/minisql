package com.minisql.backend.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.primitives.Bytes;

public class ByteUtil {
    private ByteUtil() {
    }

    // ----------------- short -----------------
    public static byte[] shortToByte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    // ----------------- int -----------------
    public static byte[] intToByte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    // ----------------- long -----------------
    public static byte[] longToByte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    // ----------------- String (len + data) -----------------
    public static byte[] stringToByte(String str) {
        byte[] l = intToByte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static ParsedValue parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
        return new ParsedValue(str, length + 4);
    }

}
