package com.minisql.engine.transaction.xid;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.engine.storage.codec.ByteReader;
import com.minisql.engine.storage.codec.ByteWriter;
import com.minisql.engine.storage.codec.MalformedDataException;
import com.minisql.engine.storage.io.FileChannelUtil;

/**
 * 管理 XID 文件并从持久化预留区间中分配永不复用的事务 ID。
 *
 * <p>文件格式为
 * {@code [magic:4][version:4][reservedUpperBound:8][status:1]...}。
 * {@code reservedUpperBound} 是已预留 XID 区间的开区间上界。</p>
 */
public final class XidAllocator implements AutoCloseable {

    public static final long SYSTEM_XID = 0L;
    public static final int DEFAULT_RESERVATION_SIZE = 1024;
    public static final String FILE_SUFFIX = ".xid";

    static final byte STATUS_UNUSED = 0;
    static final byte STATUS_IN_PROGRESS = 1;
    static final byte STATUS_COMMITTED = 2;
    static final byte STATUS_ABORTED = 3;

    private static final int MAGIC = 0x4D584944; // "MXID"
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_SIZE =
            Integer.BYTES + Integer.BYTES + Long.BYTES;
    private static final int LEGACY_HEADER_SIZE = Long.BYTES;
    private static final String MIGRATION_SUFFIX = ".migrating";

    private final RandomAccessFile file;
    private final FileChannel channel;
    private final int reservationSize;
    private final Lock allocationLock = new ReentrantLock();
    private final Lock fileLock = new ReentrantLock();

    private long nextXid;
    private long reservedUpperBound;
    private boolean closed;

    private XidAllocator(RandomAccessFile file, FileChannel channel, int reservationSize) {
        this.file = file;
        this.channel = channel;
        if (reservationSize <= 0) {
            throw new IllegalArgumentException(
                    "reservationSize must be positive: " + reservationSize
            );
        }
        this.reservationSize = reservationSize;
        this.reservedUpperBound = readAndValidate();
        // 重启时跳过上次预留但可能尚未使用的整个区间。
        this.nextXid = reservedUpperBound;
    }

    public static XidAllocator create(String basePath) {
        return create(basePath, DEFAULT_RESERVATION_SIZE);
    }

    static XidAllocator create(String basePath, int reservationSize) {
        Path path = Path.of(basePath + FILE_SUFFIX);
        try {
            Files.createFile(path);
            RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
            FileChannel channel = file.getChannel();
            try {
                writeInitialHeader(channel);
                return new XidAllocator(file, channel, reservationSize);
            } catch (RuntimeException | IOException exception) {
                channel.close();
                file.close();
                throw exception;
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to create XID file: " + path,
                    exception
            );
        }
    }

    public static XidAllocator open(String basePath) {
        return open(basePath, DEFAULT_RESERVATION_SIZE);
    }

    static XidAllocator open(String basePath, int reservationSize) {
        Path path = Path.of(basePath + FILE_SUFFIX);
        if (!Files.exists(path)) {
            throw new IllegalStateException("XID file does not exist: " + path);
        }
        migrateLegacyFormat(path);
        try {
            RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
            FileChannel channel = file.getChannel();
            try {
                return new XidAllocator(file, channel, reservationSize);
            } catch (RuntimeException exception) {
                channel.close();
                file.close();
                throw exception;
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to open XID file: " + path,
                    exception
            );
        }
    }

    public long allocate() {
        allocationLock.lock();
        try {
            requireOpen();
            if (nextXid == reservedUpperBound) {
                reserveNextRange();
            }
            return nextXid++;
        } finally {
            allocationLock.unlock();
        }
    }

    /**
     * 返回下一次分配将使用的 XID，但不消耗 XID。
     * ReadView 使用它划分创建视图之后才开始的事务。
     */
    public long peekNextXid() {
        allocationLock.lock();
        try {
            requireOpen();
            return nextXid;
        } finally {
            allocationLock.unlock();
        }
    }

    byte readStatus(long xid) {
        fileLock.lock();
        try {
            requireOpen();
            if (xid <= SYSTEM_XID || xid >= reservedUpperBound) {
                return STATUS_UNUSED;
            }
            ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES);
            FileChannelUtil.readFully(channel, buffer, statusOffset(xid));
            return buffer.array()[0];
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to read status for XID " + xid,
                    exception
            );
        } finally {
            fileLock.unlock();
        }
    }

    void writeStatus(long xid, byte status) {
        fileLock.lock();
        try {
            requireOpen();
            requireKnownStatus(status);
            if (xid <= SYSTEM_XID || xid >= reservedUpperBound) {
                throw new IllegalArgumentException(
                        "XID is outside the reserved range: " + xid
                );
            }
            FileChannelUtil.writeFully(
                    channel,
                    ByteBuffer.wrap(new byte[] { status }),
                    statusOffset(xid)
            );
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to write status for XID " + xid,
                    exception
            );
        } finally {
            fileLock.unlock();
        }
    }

    public void force() {
        fileLock.lock();
        try {
            requireOpen();
            channel.force(false);
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to flush XID file", exception);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void close() {
        allocationLock.lock();
        fileLock.lock();
        try {
            if (closed) {
                return;
            }
            channel.force(false);
            channel.close();
            file.close();
            closed = true;
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to close XID file", exception);
        } finally {
            fileLock.unlock();
            allocationLock.unlock();
        }
    }

    private void reserveNextRange() {
        long newUpperBound;
        try {
            newUpperBound = Math.addExact(
                    reservedUpperBound,
                    reservationSize
            );
        } catch (ArithmeticException exception) {
            throw new IllegalStateException("XID space exhausted", exception);
        }

        fileLock.lock();
        try {
            long requiredSize = statusOffset(newUpperBound);
            extendWithUnusedStatuses(requiredSize);
            writeHeader(newUpperBound);
            channel.force(false);
            reservedUpperBound = newUpperBound;
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to reserve XID range", exception);
        } finally {
            fileLock.unlock();
        }
    }

    private long readAndValidate() {
        fileLock.lock();
        try {
            long fileSize = channel.size();
            if (fileSize < HEADER_SIZE) {
                throw malformed("XID file is shorter than its header");
            }
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            FileChannelUtil.readFully(channel, header, 0);
            ByteReader reader = ByteReader.wrap(header.array());
            int magic = reader.readInt();
            int version = reader.readInt();
            long upperBound = reader.readLong();
            reader.requireFullyConsumed();

            if (magic != MAGIC) {
                throw malformed("Unexpected XID magic: 0x"
                        + Integer.toHexString(magic));
            }
            if (version != FORMAT_VERSION) {
                throw malformed("Unsupported XID format version: " + version);
            }
            if (upperBound < 1) {
                throw malformed("Invalid reserved XID upper bound: " + upperBound);
            }
            long expectedSize = statusOffset(upperBound);
            if (fileSize != expectedSize) {
                throw malformed(
                        "XID file size mismatch: expected " + expectedSize
                                + " but found " + fileSize
                );
            }
            validateStatuses(upperBound);
            return upperBound;
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to validate XID file", exception);
        } finally {
            fileLock.unlock();
        }
    }

    private void validateStatuses(long upperBound) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        long position = HEADER_SIZE;
        long end = statusOffset(upperBound);
        while (position < end) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), end - position));
            int length = buffer.limit();
            FileChannelUtil.readFully(channel, buffer, position);
            byte[] bytes = buffer.array();
            for (int index = 0; index < length; index++) {
                requireKnownStatus(bytes[index]);
            }
            position += length;
        }
    }

    private void extendWithUnusedStatuses(long requiredSize) throws IOException {
        long position = channel.size();
        ByteBuffer zeroes = ByteBuffer.allocate(8192);
        while (position < requiredSize) {
            zeroes.clear();
            zeroes.limit((int) Math.min(zeroes.capacity(), requiredSize - position));
            int length = zeroes.limit();
            FileChannelUtil.writeFully(channel, zeroes, position);
            position += length;
        }
    }

    private void writeHeader(long upperBound) throws IOException {
        ByteWriter writer = ByteWriter.allocate(HEADER_SIZE);
        writer.writeInt(MAGIC);
        writer.writeInt(FORMAT_VERSION);
        writer.writeLong(upperBound);
        FileChannelUtil.writeFully(
                channel,
                ByteBuffer.wrap(writer.toByteArray()),
                0
        );
    }

    private static void writeInitialHeader(FileChannel channel) throws IOException {
        ByteWriter writer = ByteWriter.allocate(HEADER_SIZE);
        writer.writeInt(MAGIC);
        writer.writeInt(FORMAT_VERSION);
        writer.writeLong(1L);
        FileChannelUtil.writeFully(
                channel,
                ByteBuffer.wrap(writer.toByteArray()),
                0
        );
        channel.force(false);
    }

    private static long statusOffset(long xid) {
        try {
            return Math.addExact(HEADER_SIZE, Math.subtractExact(xid, 1L));
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("XID offset overflow: " + xid, exception);
        }
    }

    private static void requireKnownStatus(byte status) {
        if (status < STATUS_UNUSED || status > STATUS_ABORTED) {
            throw malformed(
                    "Unknown XID status: " + Byte.toUnsignedInt(status)
            );
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("XID allocator is closed");
        }
    }

    private static MalformedDataException malformed(String message) {
        return new MalformedDataException(message);
    }

    private static void migrateLegacyFormat(Path path) {
        Path temporaryPath = Path.of(path.toString() + MIGRATION_SUFFIX);
        try {
            Files.deleteIfExists(temporaryPath);
            byte[] source = Files.readAllBytes(path);
            if (source.length >= Integer.BYTES
                    && ByteReader.wrap(source).readInt() == MAGIC) {
                return;
            }
            if (source.length < LEGACY_HEADER_SIZE) {
                throw malformed("Legacy XID file is shorter than its header");
            }

            ByteReader reader = ByteReader.wrap(source);
            long xidCounter = reader.readLong();
            if (xidCounter < 0
                    || xidCounter != source.length - LEGACY_HEADER_SIZE) {
                throw malformed("Invalid legacy XID counter: " + xidCounter);
            }
            long upperBound = Math.addExact(xidCounter, 1L);
            int targetSize = Math.toIntExact(
                    Math.addExact(HEADER_SIZE, xidCounter)
            );
            ByteWriter writer = ByteWriter.allocate(targetSize);
            writer.writeInt(MAGIC);
            writer.writeInt(FORMAT_VERSION);
            writer.writeLong(upperBound);
            while (reader.hasRemaining()) {
                byte legacyStatus = reader.readByte();
                switch (legacyStatus) {
                    case 0:
                        writer.writeByte(STATUS_IN_PROGRESS);
                        break;
                    case 1:
                        writer.writeByte(STATUS_COMMITTED);
                        break;
                    case 2:
                        writer.writeByte(STATUS_ABORTED);
                        break;
                    default:
                        throw malformed(
                                "Unknown legacy XID status: "
                                        + Byte.toUnsignedInt(legacyStatus)
                        );
                }
            }
            reader.requireFullyConsumed();

            Files.write(temporaryPath, writer.toByteArray());
            try (RandomAccessFile temporary =
                         new RandomAccessFile(temporaryPath.toFile(), "rw")) {
                temporary.getChannel().force(false);
            }
            try {
                Files.move(
                        temporaryPath,
                        path,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING
                );
            } catch (AtomicMoveNotSupportedException exception) {
                Files.move(
                        temporaryPath,
                        path,
                        StandardCopyOption.REPLACE_EXISTING
                );
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to migrate legacy XID file: " + path,
                    exception
            );
        } catch (ArithmeticException exception) {
            throw malformed("Legacy XID file is too large");
        } finally {
            try {
                Files.deleteIfExists(temporaryPath);
            } catch (IOException ignored) {
                // The next open attempt removes a stale migration file.
            }
        }
    }
}
