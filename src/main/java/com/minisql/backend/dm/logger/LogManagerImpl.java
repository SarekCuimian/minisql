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
 *   MAGIC(4) VERSION(4)
 *   HDR_CRC(4) CHECKPOINT_LSN(8) FLUSHED_LSN(8)
 *   RESERVED(4)
 * Record:
 *   payloadSize(4) recordCRC(4) lsn(8) payload(payloadSize)
 *
 * LSN = record 在文件中的结束偏移（byte offset）
 */
public class LogManagerImpl implements LogManager {

    private static final int MAGIC = 0x524C4F47; // "RLOG"
    private static final int VERSION = 2;

    private static final int HEADER_SIZE = 32;
    private static final int RECORD_HEADER_SIZE = 16;

    private static final int DEFAULT_LOG_BUFFER_SIZE = 4 << 20; // 4MB log buffer
    private static final int WRITE_AHEAD_BUFFER_SIZE = 8 * 1024; // 8KB writer buffer
    private static final long FLUSH_INTERVAL_MS = 100L;     // flusher 最多睡 100ms

    public static final String LOG_SUFFIX = ".log";

    private final File logFile;
    private final RandomAccessFile raf;
    private final FileChannel channel;

    private final ReentrantLock lock = new ReentrantLock();

    /** buffer 有空位 */
    private final Condition notFull = lock.newCondition();

    /** buffer 有数据 */
    private final Condition notEmpty = lock.newCondition();

    /** writer 已将数据写到文件通道，唤醒 flusher 刷盘 */
    private final Condition written = lock.newCondition();

    /** flusher 已经执行 force 完成刷盘，提示 flush 动作可以结束 */
    private final Condition flushed = lock.newCondition();

    /** 日志缓冲区（环形） */
    private final RingBuffer ringBuffer;

    /** 内存可见运行标志 */
    private volatile boolean running;

    /** 下一条 record 的起始偏移（逻辑分配）*/
    private long currentLsn;

    /** writer 已写文件边界，还并未刷盘 */ 
    private long writtenLsn;

    /** 日志持久化边界：小于这个 LSN 的日志已经落盘到日志文件 */
    private long flushedLsn;

    /** 数据页持久化边界：小于这个 LSN 的修改已经落盘到数据文件，崩溃恢复只需从此 LSN 开始 REDO */
    private long checkpointLsn;

    /** 刷盘最低要求 LSN：合并并发提交的刷盘目标 LSN，取并发 commit 中最大 LSN，避免重复刷盘 */
    private long flushTargetLsn;

    /** log writer 线程 */
    private Thread writer;

    /** log flusher 线程 */
    private Thread flusher;

    public static LogManagerImpl create(String path) {
        return create(path, DEFAULT_LOG_BUFFER_SIZE);
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

        LogManagerImpl lgm = new LogManagerImpl(f, raf, fc, bufferSize);
        lgm.initLogFile();
        lgm.startWorkerThreads();
        return lgm;
    }

    public static LogManagerImpl open(String path) {
        return open(path, DEFAULT_LOG_BUFFER_SIZE);
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

        LogManagerImpl lgm = new LogManagerImpl(f, raf, fc, bufferSize);
        lgm.loadHeader();
        lgm.trimBadTail();
        lgm.startWorkerThreads();
        return lgm;
    }

    private LogManagerImpl(File logFile, RandomAccessFile raf, FileChannel channel, int bufferSize) {
        this.logFile = logFile;
        this.raf = raf;
        this.channel = channel;
        this.ringBuffer = new RingBuffer(Math.max(64, bufferSize));
    }

    /**
     * 追加一条日志到内存 buffer，返回该记录的 end LSN
     */
    @Override
    public long log(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload is null");
        }
        // 记录长度 = 记录头长度 + 负载长度
        int recordSize = RECORD_HEADER_SIZE + payload.length;
        if (recordSize > ringBuffer.capacity()) {
            // 简化实现：不支持超大 record（生产级可做 bypass buffer 直接写文件）
            throw new IllegalArgumentException("record too large: " + recordSize);
        }

        lock.lock();
        try {
            while (!ringBuffer.hasSpace(writtenLsn, currentLsn, recordSize)) {
                // buffer 不够，先唤醒 writer 尽快写走 buffer，释放空间
                notEmpty.signal();
                // append 线程等待 writer 清理 buffer 后再次发出 notFull 条件信号
                notFull.await();
            }
            // 计算本条记录的 LSN，即下一条记录的起始偏移
            long start = currentLsn;
            long lsn = start + recordSize;
            currentLsn = lsn;

            // 封装 record 并写入 buffer
            byte[] record = wrapRecord(lsn, payload);
            ringBuffer.write(start, record);

            // 写入后唤醒 writer，buffer 里有数据了，可以写文件
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
            flushTargetLsn = Math.max(flushTargetLsn, lsn);
            // 提醒 writer 拿缓冲区元素写文件
            notEmpty.signal(); 
            // 提醒 flusher 将文件刷到磁盘
            written.signal();
            // 等待 flusher 推进 flushedLsn
            while (flushedLsn < lsn) {
                // 等 flusher force，即刷到磁盘后，继续执行
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
    public LogManager.LogReader getReader() {
        return new LogReader(logFile);
    }

    @Override
    public void close() {
        // 保证 LSN <= currentLsn 的日志都落盘
        flush(currentLsn);
        // 停止后台线程
        running = false;

        lock.lock();
        try {
            // 关闭时唤醒所有可能在 await 的线程，防止卡死
            notFull.signalAll();   // append 可能在等空间
            notEmpty.signalAll();  // writer 可能在等数据
            written.signalAll();   // flusher 可能在等 written 事件
            flushed.signalAll();   // flush 调用者可能在等 durable
        } finally {
            lock.unlock();
        }

        awaitWorkerThreads();

        try {
            channel.close();
            raf.close();
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    private void startWorkerThreads() {
        running = true;
        writer = new Thread(new LogWriter(), "LogWriter");
        flusher = new Thread(new LogFlusher(), "LogFlusher");
        writer.start();
        flusher.start();
    }

    private void awaitWorkerThreads() {
        try {
            writer.join();
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * writer：把 ringBuffer 中的数据写入文件（write），推进 writtenLsn
     */
    private final class LogWriter implements Runnable {
        
        private final ByteBuffer writeAheadBuffer = ByteBuffer.allocate(WRITE_AHEAD_BUFFER_SIZE);

        @Override
        public void run() {
            while (running) {
                // 从环形缓冲区读出到 chunk 暂存
                long offset;
                int len;

                lock.lock();
                try {
                    while (running && currentLsn == writtenLsn) {
                        // buffer 为空：writer 等待 append/flush signal(notEmpty)
                        notEmpty.await();
                    }
                    if (!running && currentLsn == writtenLsn) {
                        return;
                    }

                    // 拷贝 ringBuffer 中未写入的数据（释放锁后做 IO）
                    long available = currentLsn - writtenLsn;
                    len = (int) Math.min(available, writeAheadBuffer.capacity());
                    // 每次从 log buffer 读取先清理 write ahead buffer
                    writeAheadBuffer.clear();
                    ringBuffer.read(writtenLsn, len, writeAheadBuffer);
                    writeAheadBuffer.flip();

                    // 文件写入偏移（文件尾）
                    offset = writtenLsn;

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    lock.unlock();
                }

                // 不持锁做 IO
                try {
                    // 把写前缓冲区的内容写入文件通道
                    FileChannelUtil.writeFully(channel, writeAheadBuffer, offset);
                } catch (IOException e) {
                    Panic.of(e);
                }

                lock.lock();
                try {
                    // 推进 written 边界（注意：此时可能还没 force）
                    writtenLsn += len;
                    // buffer 空间已释放：唤醒等待空间的 append
                    notFull.signalAll();
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
                    // writtenLsn <= flushedLsn
                    // 没有新日志写到文件里，没东西可刷，继续等
                    // flushTargetLsn > 0 && writtenLsn < flushTargetLsn
                    // 有事务要求刷到某个 LSN，但 writer 还没把日志写到那里，刷也没意义，继续等
                    while (running && (writtenLsn <= flushedLsn
                                    || (flushTargetLsn > 0 && writtenLsn < flushTargetLsn))) {
                        // await(timeout) 即便没被 signal，也会周期性醒来做一次检查
                        written.await(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
                    }
                    // 运行标志为 false 时，提前返回，停止运行
                    if (!running) return;
                    // 如果醒来后发现还是没有新内容，就继续 run 循环
                    if (writtenLsn <= flushedLsn) continue;

                    // 本轮 flush 的目标是当前 writtenLsn
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
                    // 将 FileChannel 中数据最终刷入磁盘
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
                    if (flushTargetLsn > 0 && flushedLsn >= flushTargetLsn) {
                        flushTargetLsn = 0;
                    }

                    // 唤醒所有等待 flush(lsn) 的线程：flushedLsn 更新了
                    flushed.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * 初始化 log 文件，写入 header 各字段初始值
     */
    private void initLogFile() {
        // header 中存储的字段
        checkpointLsn = HEADER_SIZE;
        flushedLsn = HEADER_SIZE;
        // 内存中存储的字段
        currentLsn = HEADER_SIZE;
        writtenLsn = HEADER_SIZE;
        flushTargetLsn = 0;
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
        // limit = HEADER_SIZE, position = 0, 进入读模式
        buf.flip();

        int magic = buf.getInt();
        int version = buf.getInt();
        int checksum = buf.getInt();
        long checkpoint = buf.getLong();
        long flushed = buf.getLong();
        buf.getInt(); // reserved

        // 检查 header
        if (magic != MAGIC || version != VERSION) {
            Panic.of(Error.BadLogFileException);
        }
        if (checksum != calcHeaderChecksum(checkpoint, flushed)) {
            Panic.of(Error.BadLogFileException);
        }

        this.checkpointLsn = checkpoint;
        this.flushedLsn = flushed;

        // 先假设文件尾都在，后续 trimBadTail 会截断文件坏尾并修正
        currentLsn = size;
        writtenLsn = size;
        flushTargetLsn = 0;
    }

    /**
     * 启动时扫 record，遇到坏尾巴就 truncate
     */
    private void trimBadTail() {
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
            long endLsn = header.getLong();
            long recordEnd = pos + RECORD_HEADER_SIZE + payloadLen;

            // lsn 必须等于 record 结束位置（防止错位）
            if (payloadLen < 0 || endLsn != recordEnd) {
                break;
            }
            if (recordEnd > size) {
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
            int calc = calcRecordChecksum(endLsn, payload);
            if (calc != checksum) {
                break;
            }

            pos = recordEnd;
            lastValid = recordEnd;
        }

        if (lastValid < size) {
            try {
                channel.truncate(lastValid);
            } catch (IOException e) {
                Panic.of(e);
            }
        }

        currentLsn = lastValid;
        writtenLsn = lastValid;

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
        byte[] check = ByteUtil.longToByte(checkpoint);
        byte[] flush = ByteUtil.longToByte(flushed);
        crc.update(check, 0, check.length);
        crc.update(flush, 0, flush.length);
        return (int) crc.getValue();
    }

    private static int calcRecordChecksum(long lsn, byte[] payload) {
        CRC32C crc = new CRC32C();
        byte[] lsnRaw = ByteUtil.longToByte(lsn);
        crc.update(lsnRaw, 0, lsnRaw.length);
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }


    /**
     * 只读 reader，用于启动恢复，默认 fileSize 固定为打开时长度
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

            int payloadSize = header.getInt();
            int checksum = header.getInt();
            long endLsn = header.getLong();
            long recordEnd = position + RECORD_HEADER_SIZE + payloadSize;

            if (payloadSize < 0
                || endLsn != recordEnd
                || recordEnd > fileSize) {
                return null;
            }

            ByteBuffer data = ByteBuffer.allocate(payloadSize);
            try {
                FileChannelUtil.readFully(channel, data, position + RECORD_HEADER_SIZE);
            } catch (IOException e) {
                Panic.of(e);
            }

            byte[] payload = data.array();
            int calc = calcRecordChecksum(endLsn, payload);
            if (calc != checksum) {
                return null;
            }

            position = recordEnd;
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
