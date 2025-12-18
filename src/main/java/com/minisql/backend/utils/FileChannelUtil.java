package com.minisql.backend.utils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * FileChannel 读写工具，保证读写完整缓冲区。
 */
public final class FileChannelUtil {
    private FileChannelUtil() {
    }

    /**
     * 循环读取直到填满 {@code buf}，使用定位读避免共享 position 的并发竞态
     */
    public static void readFully(FileChannel fc, ByteBuffer buf, long offset) throws IOException {
        long pos = offset;
        while (buf.hasRemaining()) {
            int n = fc.read(buf, pos);
            if (n < 0) {
                throw new EOFException("Unexpected EOF at pos=" + pos);
            }
            pos += n;
        }
    }

    /**
     * 循环写入直到写完 {@code buf}，使用定位写避免共享 position 的并发竞态
     */
    public static void writeFully(FileChannel fc, ByteBuffer buf, long offset) throws IOException {
        long pos = offset;
        while (buf.hasRemaining()) {
            int n = fc.write(buf, pos);
            pos += n;
        }
    }

}