package com.minisql.backend.dm.logger;

import java.nio.ByteBuffer;

/**
 * 环形日志缓冲区
 */
public final class RingBuffer {

    private final byte[] buf;
    private final int capacity;

    public RingBuffer(int capacity) {
        this.capacity = capacity;
        this.buf = new byte[capacity];
    }

    public int capacity() {
        return capacity;
    }

    public boolean hasSpace(long writtenLsn, long currentLsn, int len) {
        return (currentLsn - writtenLsn + len) <= capacity;
    }

    /**
     * 写日志缓冲区
     *
     * @param start 写入的起始位置
     * @param src 待写入的日志
     */
    public void write(long start, byte[] src) {
        int len = src.length;
        int pos = (int) (start % capacity);

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
     * @param start 读取的起始位置
     * @param len 读取的长度
     * @param dst 存放读取结果
     */
    public void read(long start, int len, ByteBuffer dst) {
        int pos = (int) (start % capacity);

        if (pos + len <= capacity) {
            dst.put(buf, pos, len);
        } else {
            int first = capacity - pos;
            dst.put(buf, pos, first);
            dst.put(buf, 0, len - first);
        }
    }
}
