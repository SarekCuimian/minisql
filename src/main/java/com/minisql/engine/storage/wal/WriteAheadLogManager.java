package com.minisql.engine.storage.wal;

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

import com.minisql.engine.storage.codec.ByteReader;
import com.minisql.engine.storage.codec.ByteWriter;
import com.minisql.engine.storage.io.FileChannelUtil;
import com.minisql.error.Panic;
import com.minisql.error.Error;

/**
 * 物理 Write-Ahead Log 管理器，负责 WAL 的 append / writer / flusher 三阶段。
 *
 * 文件格式：
 * Header(32B):
 *   MAGIC(4) VERSION(4)
 *   HDR_CRC(4) CHECKPOINT_LSN(8) FLUSHED_LSN(8)
 *   RESERVED(4)
 * Record:
 *   payloadLength(4) recordCRC(4) endLsn(8) payload(payloadLength)
 *
 * LSN = record 在文件中的结束偏移（byte offset）
 */
public final class WriteAheadLogManager implements AutoCloseable {

    private static final int MAGIC = 0x524C4F47; // "RLOG"
    private static final int VERSION = 5;

    /** WAL 文件头固定占用的字节数。 */
    private static final int WAL_HEADER_SIZE = 32;
    /** WAL record header 固定占用的字节数。 */
    private static final int WAL_RECORD_HEADER_SIZE = 16;

    /** 默认 WAL ring buffer 容量：4 MiB。 */
    private static final int DEFAULT_LOG_BUFFER_CAPACITY = 4 << 20;
    /** LogWriter 单次聚合写入使用的 buffer 容量：8 KiB。 */
    private static final int WRITE_BUFFER_CAPACITY = 8 * 1024;
    private static final long FLUSH_INTERVAL_MS = 100L;     // flusher 最多睡 100ms

    public static final String LOG_SUFFIX = ".log";

    private final File logFile;
    private final RandomAccessFile raf;
    private final FileChannel channel;

    private final ReentrantLock lock = new ReentrantLock();
    private final ReentrantLock headerIoLock = new ReentrantLock();

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

    /** 当前 record 的结尾，下一条 record 的起始偏移（逻辑分配）*/
    private long currentLsn;

    /** writer 已写文件边界，还并未刷盘 */ 
    private long writtenLsn;

    /** 日志持久化边界：小于等于这个 LSN 的日志已经落盘到日志文件 */
    private long flushedLsn;

    /** 数据页持久化边界：小于等于这个 LSN 的修改已经落盘到数据文件，崩溃恢复只需从此 LSN 开始 REDO */
    private long checkpointLsn;

    /** 刷盘最低要求 LSN：合并并发提交的刷盘目标 LSN，取并发 commit 中最大 LSN，避免重复刷盘 */
    private long flushTargetLsn;

    /** log writer 线程 */
    private Thread writer;

    /** log flusher 线程 */
    private Thread flusher;

    public static WriteAheadLogManager create(String path) {
        return create(path, DEFAULT_LOG_BUFFER_CAPACITY);
    }

    public static WriteAheadLogManager create(String path, int bufferSize) {
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

        WriteAheadLogManager writeAheadLogManager =
                new WriteAheadLogManager(f, raf, fc, bufferSize);
        writeAheadLogManager.initLogFile();
        writeAheadLogManager.startWorkerThreads();
        return writeAheadLogManager;
    }

    public static WriteAheadLogManager open(String path) {
        return open(path, DEFAULT_LOG_BUFFER_CAPACITY);
    }

    public static WriteAheadLogManager open(String path, int bufferSize) {
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

        WriteAheadLogManager writeAheadLogManager =
                new WriteAheadLogManager(f, raf, fc, bufferSize);
        writeAheadLogManager.loadHeader();
        writeAheadLogManager.trimBadTail();
        writeAheadLogManager.startWorkerThreads();
        return writeAheadLogManager;
    }

    private WriteAheadLogManager(File logFile, RandomAccessFile raf, FileChannel channel, int bufferSize) {
        this.logFile = logFile;
        this.raf = raf;
        this.channel = channel;
        this.ringBuffer = new RingBuffer(Math.max(64, bufferSize));
    }

    /** 追加 payload，并返回包含 WAL interval 的完整物理记录。 */
    public LogRecord append(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload is null");
        }
        // 记录长度 = 记录头长度 + 负载长度
        int recordSize = WAL_RECORD_HEADER_SIZE + payload.length;
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
            // 计算本条记录的 end LSN，即下一条记录的起始偏移
            long start = currentLsn;
            long end = start + recordSize;
            currentLsn = end;

            // 封装 record 并写入 buffer
            byte[] record = wrapRecord(end, payload);
            ringBuffer.write(start, record);

            // 写入后唤醒 writer，buffer 里有数据了，可以写文件
            notEmpty.signal();

            return new LogRecord(start, end, payload);
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
     * - 刷页前：flush(pageLsn) (WAL)
     */
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

    public long getFlushedLsn() {
        lock.lock();
        try {
            return flushedLsn;
        } finally {
            lock.unlock();
        }
    }

    public long getWrittenLsn() {
        lock.lock();
        try {
            return writtenLsn;
        } finally {
            lock.unlock();
        }
    }

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
    public void setCheckpointLsn(long lsn) {
        long durableLsn;
        lock.lock();
        try {
            lsn = (lsn < WAL_HEADER_SIZE) ? WAL_HEADER_SIZE : lsn;
            if (lsn > flushedLsn) {
                throw new IllegalArgumentException(
                        "checkpointLsn exceeds flushedLsn: "
                                + lsn + " > " + flushedLsn
                );
            }
            checkpointLsn = lsn;
            durableLsn = flushedLsn;
        } finally {
            lock.unlock();
        }
        try {
            persistHeader(durableLsn);
        } catch (IOException exception) {
            Panic.of(exception);
        }
    }

    public Reader getReader() {
        return new Reader(logFile);
    }

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
        
        private final ByteBuffer writeAheadBuffer = ByteBuffer.allocate(WRITE_BUFFER_CAPACITY);

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
        public void run() {
            while (running) {
                long target;
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
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    lock.unlock();
                }

                // 不持锁做 IO：写 header + force
                try {
                    persistHeader(target);
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
        checkpointLsn = WAL_HEADER_SIZE;
        flushedLsn = WAL_HEADER_SIZE;
        // 内存中存储的字段
        currentLsn = WAL_HEADER_SIZE;
        writtenLsn = WAL_HEADER_SIZE;
        flushTargetLsn = 0;
        try {
            persistHeader(flushedLsn);
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
        if (size < WAL_HEADER_SIZE) {
            Panic.of(Error.BadLogFileException);
        }

        byte[] headerBytes = new byte[WAL_HEADER_SIZE];
        try {
            FileChannelUtil.readFully(channel, ByteBuffer.wrap(headerBytes), 0);
        } catch (IOException e) {
            Panic.of(e);
        }
        ByteReader reader = ByteReader.wrap(headerBytes);
        int magic = reader.readInt();
        int version = reader.readInt();
        int checksum = reader.readInt();
        long checkpoint = reader.readLong();
        long flushed = reader.readLong();
        reader.readInt(); // reserved
        reader.requireFullyConsumed();

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

        long pos = WAL_HEADER_SIZE;
        long lastValid = pos;

        byte[] headerBytes = new byte[WAL_RECORD_HEADER_SIZE];
        ByteBuffer header = ByteBuffer.wrap(headerBytes);
        while (pos + WAL_RECORD_HEADER_SIZE <= size) {
            header.clear();
            try {
                FileChannelUtil.readFully(channel, header, pos);
            } catch (IOException e) {
                Panic.of(e);
                break;
            }
            ByteReader reader = ByteReader.wrap(headerBytes);
            int payloadLength = reader.readInt();
            int checksum = reader.readInt();
            long endLsn = reader.readLong();
            reader.requireFullyConsumed();
            long recordEnd = pos + WAL_RECORD_HEADER_SIZE + payloadLength;

            // lsn 必须等于 record 结束位置（防止错位）
            if (payloadLength < 0 || endLsn != recordEnd) {
                break;
            }
            if (recordEnd > size) {
                break;
            }

            ByteBuffer data = ByteBuffer.allocate(payloadLength);
            try {
                FileChannelUtil.readFully(channel, data, pos + WAL_RECORD_HEADER_SIZE);
            } catch (IOException e) {
                Panic.of(e);
                break;
            }

            byte[] payload = data.array();
            int calc = calcRecordChecksum(payloadLength, endLsn, payload);
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
        ByteWriter writer = ByteWriter.allocate(WAL_HEADER_SIZE);
        writer.writeInt(MAGIC);
        writer.writeInt(VERSION);
        writer.writeInt(checksum);
        writer.writeLong(checkpoint);
        writer.writeLong(flushed);
        writer.writeInt(0); // reserved
        FileChannelUtil.writeFully(channel, ByteBuffer.wrap(writer.toByteArray()), 0);
    }

    private void persistHeader(long durableLsn) throws IOException {
        headerIoLock.lock();
        try {
            long durableCheckpointLsn;
            lock.lock();
            try {
                durableCheckpointLsn = Math.min(checkpointLsn, durableLsn);
            } finally {
                lock.unlock();
            }
            writeHeader(durableCheckpointLsn, durableLsn);
            channel.force(false);
        } finally {
            headerIoLock.unlock();
        }
    }

    private static byte[] wrapRecord(long lsn, byte[] payload) {
        int checksum = calcRecordChecksum(payload.length, lsn, payload);
        ByteWriter writer = ByteWriter.allocate(WAL_RECORD_HEADER_SIZE + payload.length);
        writer.writeInt(payload.length);
        writer.writeInt(checksum);
        writer.writeLong(lsn);
        writer.writeBytes(payload);
        return writer.toByteArray();
    }

    private static int calcHeaderChecksum(long checkpoint, long flushed) {
        CRC32C crc = new CRC32C();
        ByteWriter writer = ByteWriter.allocate(
                3 * Integer.BYTES + 2 * Long.BYTES
        );
        writer.writeInt(MAGIC);
        writer.writeInt(VERSION);
        writer.writeLong(checkpoint);
        writer.writeLong(flushed);
        writer.writeInt(0); // reserved
        byte[] checksumBytes = writer.toByteArray();
        crc.update(checksumBytes, 0, checksumBytes.length);
        return (int) crc.getValue();
    }

    private static int calcRecordChecksum(int payloadLength, long endLsn, byte[] payload) {
        CRC32C crc = new CRC32C();
        ByteWriter writer = ByteWriter.allocate(Integer.BYTES + Long.BYTES);
        writer.writeInt(payloadLength);
        writer.writeLong(endLsn);
        byte[] headerBytes = writer.toByteArray();
        crc.update(headerBytes, 0, headerBytes.length);
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }


    /**
     * 只读 reader，用于启动恢复，默认 fileSize 固定为打开时长度
     * 理论上要具备从checkpoint开始读的能力
     */
    public static final class Reader implements AutoCloseable {
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final long fileSize;
        private long position;

        private Reader(File file) {
            try {
                this.raf = new RandomAccessFile(file, "r");
                this.channel = raf.getChannel();
                this.fileSize = raf.length();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.position = WAL_HEADER_SIZE;
        }

        public LogRecord next() {
            if (position + WAL_RECORD_HEADER_SIZE > fileSize) {
                return null;
            }

            long startLsn = position;
            byte[] headerBytes = new byte[WAL_RECORD_HEADER_SIZE];
            try {
                FileChannelUtil.readFully(channel, ByteBuffer.wrap(headerBytes), position);
            } catch (IOException e) {
                Panic.of(e);
            }
            ByteReader reader = ByteReader.wrap(headerBytes);
            int payloadLength = reader.readInt();
            int checksum = reader.readInt();
            long endLsn = reader.readLong();
            reader.requireFullyConsumed();
            long recordEnd = position + WAL_RECORD_HEADER_SIZE + payloadLength;

            if (payloadLength < 0
                || endLsn != recordEnd
                || recordEnd > fileSize) {
                return null;
            }

            ByteBuffer data = ByteBuffer.allocate(payloadLength);
            try {
                FileChannelUtil.readFully(channel, data, position + WAL_RECORD_HEADER_SIZE);
            } catch (IOException e) {
                Panic.of(e);
            }

            byte[] payload = data.array();
            int calc = calcRecordChecksum(payloadLength, endLsn, payload);
            if (calc != checksum) {
                return null;
            }

            position = recordEnd;
            return new LogRecord(startLsn, endLsn, payload);
        }

        public void rewind() {
            position = WAL_HEADER_SIZE;
        }

        public void seek(long lsn) {
            position = Math.max(lsn, WAL_HEADER_SIZE);
        }

        public long position() {
            return position;
        }

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
