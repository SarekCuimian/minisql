package com.minisql.backend.dm.logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

import com.minisql.backend.utils.ByteUtil;
import com.minisql.backend.utils.FileChannelUtil;
import com.minisql.backend.utils.Panic;
import com.minisql.common.Error;

/**
 * 日志管理器（append / writer / flusher 三阶段）
 *
 * 文件格式：
 * Header(32B):
 *   MAGIC(4) VERSION(4) HDR_CRC(4) CHECKPOINT_LSN(8) FLUSHED_LSN(8) RESERVED(4)
 * Record:
 *   payloadLen(4) recordCRC(4) lsn(8) payload(payloadLen)
 *
 * LSN = record 在文件中的起始偏移（byte offset）
 */
public class LogManagerImpl implements LogManager {

    private static final int MAGIC = 0x524C4F47; // "RLOG"
    private static final int VERSION = 1;

    private static final int HEADER_SIZE = 32;
    private static final int RECORD_HEADER_SIZE = 16;

    private static final int DEFAULT_BUFFER_SIZE = 4 << 20; // 4MB log buffer
    private static final long FLUSH_INTERVAL_MS = 100L;     // flusher 最多睡 100ms

    public static final String LOG_SUFFIX = ".log";

    private final File logFile;
    private final RandomAccessFile raf;
    private final FileChannel channel;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * notFull：append 线程等“buffer 有空位”。
     * - await：append 在 buffer 空间不足时等待
     * - signal：writer 清空/写走 buffer 后唤醒 append
     */
    private final Condition notFull = lock.newCondition();

    /**
     * notEmpty：writer 线程等“buffer 有数据可写”。
     * - await：writer 在 buffer 为空时等待
     * - signal：append 写入 buffer 后唤醒 writer；flush 也会唤醒 writer（加速）
     */
    private final Condition notEmpty = lock.newCondition();

    /**
     * written：flusher 线程等“writtenLsn 推进（新数据已 write 到文件）”。
     * - await：flusher 等 writer 写出新数据（或等 flushRequiredLsn 满足）
     * - signal：writer 写完文件后唤醒 flusher；flush/setCheckpoint 也会唤醒 flusher（加速检查）
     */
    private final Condition written = lock.newCondition();

    /**
     * flushed：flush(lsn) 的调用线程等“flushedLsn >= lsn（已经 durable）”。
     * - await：flush 调用线程等待 durable
     * - signal：flusher force 完成并推进 flushedLsn 后唤醒
     */
    private final Condition flushed = lock.newCondition();

    private final ByteBuffer logBuffer;

    private volatile boolean running;

    // nextLsn：下一条 record 的起始偏移（逻辑分配）
    private long nextLsn;

    // writeOffset：writer 下一次写文件的偏移（文件尾）
    private long writeOffset;

    // writtenLsn：已经 write 到文件完成的边界（可能还没 force）
    private long writtenLsn;

    // flushedLsn：已经 force 到磁盘的边界（durable）
    private long flushedLsn;

    // checkpointLsn：写入 header 的 checkpoint（恢复起点）。这里只负责持久化值，真正刷页由外部保证
    private long checkpointLsn;

    // flushRequiredLsn：合并并发提交的刷盘目标lsn，取并发commit的最大lsn，避免重复刷盘
    private long flushRequiredLsn;

    private Thread writer;
    private Thread flusher;

    public static LogManagerImpl create(String path) {
        return create(path, DEFAULT_BUFFER_SIZE);
    }

    public static LogManagerImpl create(String path, int bufferSize) {
        File f = new File(path + LOG_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.of(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.of(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.of(e);
        }

        LogManagerImpl lm = new LogManagerImpl(f, raf, fc, bufferSize);
        lm.initNewFile();
        lm.startBackgroundThreads();
        return lm;
    }

    public static LogManagerImpl open(String path) {
        return open(path, DEFAULT_BUFFER_SIZE);
    }

    public static LogManagerImpl open(String path, int bufferSize) {
        File f = new File(path + LOG_SUFFIX);
        if (!f.exists()) {
            Panic.of(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.of(e);
        }

        LogManagerImpl lm = new LogManagerImpl(f, raf, fc, bufferSize);
        lm.loadHeader();
        lm.trimTail();
        lm.startBackgroundThreads();
        return lm;
    }

    private LogManagerImpl(File logFile, RandomAccessFile raf, FileChannel channel, int bufferSize) {
        this.logFile = logFile;
        this.raf = raf;
        this.channel = channel;
        this.logBuffer = ByteBuffer.allocate(Math.max(64, bufferSize));
    }

    /**
     * 追加一条日志到内存 buffer，返回该记录的 LSN（文件起始 offset）。
     * 注意：这里只保证“分配了 LSN 并写入 buffer”，不保证 durable。
     */
    @Override
    public long append(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload is null");
        }

        int recordSize = RECORD_HEADER_SIZE + payload.length;
        if (recordSize > logBuffer.capacity()) {
            // 简化实现：不支持超大 record（生产级可做 bypass buffer 直接写文件）
            throw new IllegalArgumentException("record too large: " + recordSize);
        }

        lock.lock();
        try {
            while (recordSize > logBuffer.remaining()) {
                // buffer 不够：先唤醒 writer 尽快写走 buffer，释放空间
                // （如果 writer 正在 notEmpty.await() 睡着，这里能把它叫醒）
                notEmpty.signal();

                // append 线程等待：直到 writer 清空 buffer 后 signal notFull
                notFull.await();
            }

            // 分配 LSN（LSN=record 起始 offset）
            long lsn = nextLsn;
            nextLsn += recordSize;

            // 封装 record 并写入 buffer
            byte[] record = wrapRecord(lsn, payload);
            logBuffer.put(record);

            // 写入后唤醒 writer：buffer 里有数据了，可以写文件
            notEmpty.signal();

            return lsn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻塞直到 durable：保证返回时 flushedLsn >= lsn
     * 用途：
     * - commit：flush(commitLsn)
     * - 刷页前：flush(pageLSN) (WAL)
     */
    @Override
    public void flush(long lsn) {
        lock.lock();
        try {
            if (lsn <= flushedLsn) {
                // 已经 durable 到目标 lsn，直接返回
                return;
            }

            // 记录“至少要 flush 到哪里”的需求（多线程合并）
            flushRequiredLsn = Math.max(flushRequiredLsn, lsn);

            // 唤醒 writer：如果 writer 因 buffer 为空在 notEmpty.await() 睡着，叫醒它写出已有日志
            // （buffer 为空也没关系，writer 醒来会继续 await）
            notEmpty.signal();

            // 唤醒 flusher：如果 flusher 在 written.await(timeout) 睡着，叫醒它尽快检查是否需要 force
            // 这不是“已经 written”的意思，只是“快醒来看看是否该 force”
            written.signal();

            // 等待 flusher 推进 flushedLsn
            while (flushedLsn < lsn) {
                // 等 flusher force 完后 signalAll(flushed)
                flushed.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getFlushedLsn() {
        lock.lock();
        try {
            return flushedLsn;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getWrittenLsn() {
        lock.lock();
        try {
            return writtenLsn;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getCheckpointLsn() {
        lock.lock();
        try {
            return checkpointLsn;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 更新 checkpointLsn（这里只负责把值持久化到 header，真正的“刷脏页”应由外部保证）。
     * 由于 header 只在 flusher 中写入，因此这里唤醒 flusher 尽快写 header + force。
     */
    @Override
    public void setCheckpointLsn(long lsn) {
        lock.lock();
        try {
            if (lsn < HEADER_SIZE) lsn = HEADER_SIZE;
            // header 中的 checkpoint 不应超过 durable 边界
            checkpointLsn = Math.min(lsn, flushedLsn);

            // 唤醒 flusher 尽快把新 header 持久化（否则要等 timeout）
            written.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public LogManager.LogReader openReader() {
        return new LogReader(logFile);
    }

    @Override
    public void close() {
        // 先强制把已分配的所有 LSN durable（flush 是阻塞的）
        flush(nextLsn);

        // 停止后台线程
        running = false;

        lock.lock();
        try {
            // 关闭时唤醒所有可能在 await 的线程，防止卡死
            notFull.signalAll(); // append 可能在等空间
            notEmpty.signalAll();  // writer 可能在等数据
            written.signalAll();   // flusher 可能在等 written 事件
            flushed.signalAll();   // flush 调用者可能在等 durable
        } finally {
            lock.unlock();
        }

        joinThread(writer);
        joinThread(flusher);

        try {
            channel.close();
            raf.close();
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    private void startBackgroundThreads() {
        running = true;
        writer = new Thread(new LogWriter(), "LogWriter");
        flusher = new Thread(new LogFlusher(), "LogFlusher");
        writer.start();
        flusher.start();
    }

    /**
     * writer：把 logBuffer 中的数据写入文件（write），推进 writtenLsn
     */
    private final class LogWriter implements Runnable {
        @Override
        public void run() {
            while (running) {
                byte[] chunk;
                long offset;

                lock.lock();
                try {
                    while (running && logBuffer.position() == 0) {
                        // buffer 为空：writer 等待 append/flush signal(notEmpty)
                        notEmpty.await();
                    }
                    if (!running && logBuffer.position() == 0) {
                        return;
                    }

                    // 拷贝 buffer 当前内容到 chunk（释放锁后做 IO）
                    int len = logBuffer.position();
                    chunk = new byte[len];
                    logBuffer.flip();
                    logBuffer.get(chunk);
                    logBuffer.clear();

                    // 文件写入偏移
                    offset = writeOffset;

                    // buffer 已清空：唤醒所有在 notFull.await() 的 append 线程继续写入
                    notFull.signalAll();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    lock.unlock();
                }

                // 不持锁做 IO
                try {
                    FileChannelUtil.writeFully(channel, ByteBuffer.wrap(chunk), offset);
                } catch (IOException e) {
                    Panic.of(e);
                }

                lock.lock();
                try {
                    // 推进 written 边界（注意：此时可能还没 force）
                    writeOffset += chunk.length;
                    writtenLsn = writeOffset;

                    // 通知 flusher：有新数据已写到文件，可以检查是否需要 force
                    written.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * flusher：把 written 的数据 force 到磁盘（durable），推进 flushedLsn，并持久化 header
     */
    private final class LogFlusher implements Runnable {
        @Override
        public void run() {
            while (running) {
                long target;
                long checkpoint;

                lock.lock();
                try {
                    while (running && (writtenLsn <= flushedLsn
                            || (flushRequiredLsn > 0 && writtenLsn < flushRequiredLsn))) {
                        // flusher 等待两类条件：
                        // 1) writer 写出了新数据：writtenLsn > flushedLsn
                        // 2) 外部要求 flushRequiredLsn，但 writer 还没写到那么远：writtenLsn < flushRequiredLsn
                        //
                        // await(timeout)：既能被 signal 提前唤醒，也能定时醒来降低延迟
                        written.await(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
                    }
                    if (!running) return;

                    if (writtenLsn <= flushedLsn) {
                        // 没有需要 force 的内容
                        continue;
                    }

                    // 本轮 force 到当前 writtenLsn（可能会超过 flushRequiredLsn，允许）
                    target = writtenLsn;
                    checkpoint = Math.min(checkpointLsn, target);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    lock.unlock();
                }

                // 不持锁做 IO：写 header + force
                try {
                    writeHeader(checkpoint, target);
                    channel.force(false);
                } catch (IOException e) {
                    Panic.of(e);
                }

                lock.lock();
                try {
                    // 推进 durable 边界
                    if (target > flushedLsn) {
                        flushedLsn = target;
                    }

                    // 如果满足了外部强制 flush 请求，则清掉需求
                    if (flushRequiredLsn > 0 && flushedLsn >= flushRequiredLsn) {
                        flushRequiredLsn = 0;
                    }

                    // 唤醒所有等待 flush(lsn) 的线程：flushedLsn 更新了
                    flushed.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private void initNewFile() {
        checkpointLsn = HEADER_SIZE;
        flushedLsn = HEADER_SIZE;
        writeOffset = HEADER_SIZE;
        writtenLsn = HEADER_SIZE;
        nextLsn = HEADER_SIZE;
        flushRequiredLsn = 0;

        try {
            writeHeader(checkpointLsn, flushedLsn);
            channel.force(false);
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    private void loadHeader() {
        long size;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.of(e);
            return;
        }
        if (size < HEADER_SIZE) {
            Panic.of(Error.BadLogFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
        try {
            FileChannelUtil.readFully(channel, buf, 0);
        } catch (IOException e) {
            Panic.of(e);
        }
        buf.flip();

        int magic = buf.getInt();
        int version = buf.getInt();
        int checksum = buf.getInt();
        long ckpt = buf.getLong();
        long flushed = buf.getLong();
        buf.getInt(); // reserved

        if (magic != MAGIC || version != VERSION) {
            Panic.of(Error.BadLogFileException);
        }
        if (checksum != calcHeaderChecksum(ckpt, flushed)) {
            Panic.of(Error.BadLogFileException);
        }

        checkpointLsn = ckpt;
        flushedLsn = flushed;

        // 先假设文件尾都在（后续 trimTail 会截断坏尾巴并修正）
        writeOffset = size;
        writtenLsn = size;
        nextLsn = size;
        flushRequiredLsn = 0;
    }

    /**
     * 启动时扫 record，遇到坏尾巴就 truncate
     */
    private void trimTail() {
        long size;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.of(e);
            return;
        }

        long pos = HEADER_SIZE;
        long lastValid = pos;

        ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
        while (pos + RECORD_HEADER_SIZE <= size) {
            header.clear();
            try {
                FileChannelUtil.readFully(channel, header, pos);
            } catch (IOException e) {
                Panic.of(e);
                break;
            }
            header.flip();

            int payloadLen = header.getInt();
            int checksum = header.getInt();
            long lsn = header.getLong();

            // lsn 必须等于 record 起始位置（防止错位）
            if (payloadLen < 0 || lsn != pos) {
                break;
            }
            if (pos + RECORD_HEADER_SIZE + payloadLen > size) {
                break;
            }

            ByteBuffer data = ByteBuffer.allocate(payloadLen);
            try {
                FileChannelUtil.readFully(channel, data, pos + RECORD_HEADER_SIZE);
            } catch (IOException e) {
                Panic.of(e);
                break;
            }

            byte[] payload = data.array();
            int calc = calcRecordChecksum(lsn, payload);
            if (calc != checksum) {
                break;
            }

            pos += RECORD_HEADER_SIZE + payloadLen;
            lastValid = pos;
        }

        if (lastValid < size) {
            try {
                channel.truncate(lastValid);
            } catch (IOException e) {
                Panic.of(e);
            }
        }

        writeOffset = lastValid;
        writtenLsn = lastValid;
        nextLsn = lastValid;

        // durable 边界不能超过有效尾部
        flushedLsn = Math.min(flushedLsn, lastValid);
        checkpointLsn = Math.min(checkpointLsn, flushedLsn);
    }

    private void writeHeader(long checkpoint, long flushed) throws IOException {
        int checksum = calcHeaderChecksum(checkpoint, flushed);
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
        buf.putInt(MAGIC);
        buf.putInt(VERSION);
        buf.putInt(checksum);
        buf.putLong(checkpoint);
        buf.putLong(flushed);
        buf.putInt(0); // reserved
        buf.flip();
        FileChannelUtil.writeFully(channel, buf, 0);
    }

    private static byte[] wrapRecord(long lsn, byte[] payload) {
        int checksum = calcRecordChecksum(lsn, payload);
        ByteBuffer buf = ByteBuffer.allocate(RECORD_HEADER_SIZE + payload.length);
        buf.putInt(payload.length);
        buf.putInt(checksum);
        buf.putLong(lsn);
        buf.put(payload);
        return buf.array();
    }

    private static int calcHeaderChecksum(long checkpoint, long flushed) {
        CRC32C crc = new CRC32C();
        byte[] ckpt = ByteUtil.longToByte(checkpoint);
        byte[] fl = ByteUtil.longToByte(flushed);
        crc.update(ckpt, 0, ckpt.length);
        crc.update(fl, 0, fl.length);
        return (int) crc.getValue();
    }

    private static int calcRecordChecksum(long lsn, byte[] payload) {
        CRC32C crc = new CRC32C();
        byte[] lsnRaw = ByteUtil.longToByte(lsn);
        crc.update(lsnRaw, 0, lsnRaw.length);
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }

    private static void joinThread(Thread t) {
        if (t == null) return;
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 只读 reader（主要用于启动恢复）。默认 fileSize 固定为打开时长度。
     * 如果你想在线读增长的日志，把 fileSize 改成每次 next() 前 raf.length() 刷新即可。
     */
    private static final class LogReader implements LogManager.LogReader {
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final long fileSize;
        private long position;

        private LogReader(File file) {
            try {
                this.raf = new RandomAccessFile(file, "r");
                this.channel = raf.getChannel();
                this.fileSize = raf.length();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.position = HEADER_SIZE;
        }

        @Override
        public byte[] next() {
            if (position + RECORD_HEADER_SIZE > fileSize) {
                return null;
            }

            ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            try {
                FileChannelUtil.readFully(channel, header, position);
            } catch (IOException e) {
                Panic.of(e);
            }
            header.flip();

            int payloadLen = header.getInt();
            int checksum = header.getInt();
            long lsn = header.getLong();

            if (payloadLen < 0 || lsn != position || position + RECORD_HEADER_SIZE + payloadLen > fileSize) {
                return null;
            }

            ByteBuffer data = ByteBuffer.allocate(payloadLen);
            try {
                FileChannelUtil.readFully(channel, data, position + RECORD_HEADER_SIZE);
            } catch (IOException e) {
                Panic.of(e);
            }

            byte[] payload = data.array();
            int calc = calcRecordChecksum(lsn, payload);
            if (calc != checksum) {
                return null;
            }

            position += RECORD_HEADER_SIZE + payloadLen;
            return payload;
        }

        @Override
        public void rewind() {
            position = HEADER_SIZE;
        }

        @Override
        public void seek(long lsn) {
            position = Math.max(lsn, HEADER_SIZE);
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void close() {
            try {
                channel.close();
                raf.close();
            } catch (IOException e) {
                Panic.of(e);
            }
        }
    }
}
