package com.minisql.backend.dm.logger;

import java.nio.ByteBuffer;

/**
 * 环形日志缓冲区
 */
final class RingBuffer {

    private final byte[] buf;
    private final int capacity;

    RingBuffer(int capacity) {
        this.capacity = capacity;
        this.buf = new byte[capacity];
    }

    int capacity() {
        return capacity;
    }

    boolean hasSpace(long writtenLsn, long currentLsn, int len) {
        return (currentLsn - writtenLsn + len) <= capacity;
    }

    /**
     * 写日志缓冲区
     *
     * @param startLsn 写入的起始位置
     * @param src 待写入的日志
     */
    void write(long startLsn, byte[] src) {
        int len = src.length;
        int pos = (int) (startLsn % capacity);

        if (pos + len <= capacity) {
            System.arraycopy(src, 0, buf, pos, len);
        } else {
            int first = capacity - pos;
            System.arraycopy(src, 0, buf, pos, first);
            System.arraycopy(src, first, buf, 0, len - first);
        }
    }

    /**
     * 读日志缓冲区
     *
     * @param startLsn 读取的起始位置
     * @param len 读取的长度
     * @param dst 存放读取结果
     */
    void read(long startLsn, int len, ByteBuffer dst) {
        int pos = (int) (startLsn % capacity);

        if (pos + len <= capacity) {
            dst.put(buf, pos, len);
        } else {
            int first = capacity - pos;
            dst.put(buf, pos, first);
            dst.put(buf, 0, len - first);
        }
    }
}
