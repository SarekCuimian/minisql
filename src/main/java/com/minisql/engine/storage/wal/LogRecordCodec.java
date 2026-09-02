package com.minisql.engine.storage.wal;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.minisql.engine.storage.codec.ByteReader;
import com.minisql.engine.storage.codec.ByteWriter;
import com.minisql.engine.storage.codec.MalformedDataException;

/**
 * WAL 逻辑 payload 的唯一编码与解码入口。
 *
 * <p>物理记录头由 {@link WriteAheadLogManager} 负责；本类只处理 payload 内部格式。</p>
 */
public final class LogRecordCodec {

    private static final int COMMON_HEADER_SIZE =
            Byte.BYTES + Long.BYTES + Long.BYTES;
    private static final int RECORD_LOCATION_SIZE = Integer.BYTES + Short.BYTES;
    private static final int LENGTH_FIELD_SIZE = Integer.BYTES;

    private static final int INSERT_FIXED_SIZE =
            COMMON_HEADER_SIZE + RECORD_LOCATION_SIZE + LENGTH_FIELD_SIZE;
    private static final int UPDATE_FIXED_SIZE =
            COMMON_HEADER_SIZE + RECORD_LOCATION_SIZE + 2 * LENGTH_FIELD_SIZE;
    private static final int CLR_FIXED_SIZE =
            COMMON_HEADER_SIZE
                    + Long.BYTES
                    + RECORD_LOCATION_SIZE
                    + LENGTH_FIELD_SIZE;
    private static final int CHECKPOINT_CHUNK_HEADER_SIZE =
            Byte.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;
    private static final int DPT_ENTRY_SIZE = Integer.BYTES + Long.BYTES;
    private static final int ATT_ENTRY_SIZE =
            Long.BYTES + Byte.BYTES + Long.BYTES;
    private static final int END_CHECKPOINT_SIZE =
            Byte.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    private LogRecordCodec() {
    }

    public static LogRecordType decodeType(byte[] payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        return LogRecordType.fromCode(ByteReader.wrap(payload).readByte());
    }

    /**
     * 编码 INSERT payload：
     * {@code [type][xid][pageNumber][recordOffsetInPage][recordLength][recordBytes]}。
     */
    public static byte[] encodeInsert(
            long xid,
            long prevLsn,
            int pageNumber,
            short recordOffsetInPage,
            byte[] recordBytes
    ) {
        requireLsn(prevLsn, "prevLsn");
        requirePageNumber(pageNumber);
        requireNonEmpty(recordBytes, "recordBytes");

        ByteWriter writer = ByteWriter.allocate(addExact(INSERT_FIXED_SIZE, recordBytes.length));
        writer.writeByte(LogRecordType.INSERT.getCode());
        writer.writeLong(xid);
        writer.writeLong(prevLsn);
        writer.writeInt(pageNumber);
        writer.writeShort(recordOffsetInPage);
        writer.writeInt(recordBytes.length);
        writer.writeBytes(recordBytes);
        return writer.toByteArray();
    }

    public static InsertPayload decodeInsert(byte[] payload) {
        ByteReader reader = readerFor(payload, LogRecordType.INSERT);
        long xid = reader.readLong();
        long prevLsn = readLsn(reader, "prevLsn");
        int pageNumber = reader.readInt();
        short recordOffsetInPage = reader.readShort();
        int recordLength = readLength(reader, "recordLength");
        byte[] recordBytes = reader.readBytes(recordLength);
        reader.requireFullyConsumed();

        requireDecodedPageNumber(pageNumber);
        requireDecodedNonEmpty(recordBytes, "recordBytes");
        return new InsertPayload(
                xid,
                prevLsn,
                pageNumber,
                recordOffsetInPage,
                recordBytes
        );
    }

    /**
     * 编码 UPDATE payload：
     * {@code [type][xid][pageNumber][recordOffsetInPage]
     * [beforeImageLength][beforeImage][afterImageLength][afterImage]}。
     */
    public static byte[] encodeUpdate(
            long xid,
            long prevLsn,
            int pageNumber,
            short recordOffsetInPage,
            byte[] beforeImage,
            byte[] afterImage
    ) {
        requireLsn(prevLsn, "prevLsn");
        requirePageNumber(pageNumber);
        requireNonEmpty(beforeImage, "beforeImage");
        requireNonEmpty(afterImage, "afterImage");

        int payloadSize = addExact(UPDATE_FIXED_SIZE, beforeImage.length);
        payloadSize = addExact(payloadSize, afterImage.length);
        ByteWriter writer = ByteWriter.allocate(payloadSize);
        writer.writeByte(LogRecordType.UPDATE.getCode());
        writer.writeLong(xid);
        writer.writeLong(prevLsn);
        writer.writeInt(pageNumber);
        writer.writeShort(recordOffsetInPage);
        writer.writeInt(beforeImage.length);
        writer.writeBytes(beforeImage);
        writer.writeInt(afterImage.length);
        writer.writeBytes(afterImage);
        return writer.toByteArray();
    }

    public static UpdatePayload decodeUpdate(byte[] payload) {
        ByteReader reader = readerFor(payload, LogRecordType.UPDATE);
        long xid = reader.readLong();
        long prevLsn = readLsn(reader, "prevLsn");
        int pageNumber = reader.readInt();
        short recordOffsetInPage = reader.readShort();
        int beforeImageLength = readLength(reader, "beforeImageLength");
        byte[] beforeImage = reader.readBytes(beforeImageLength);
        int afterImageLength = readLength(reader, "afterImageLength");
        byte[] afterImage = reader.readBytes(afterImageLength);
        reader.requireFullyConsumed();

        requireDecodedPageNumber(pageNumber);
        requireDecodedNonEmpty(beforeImage, "beforeImage");
        requireDecodedNonEmpty(afterImage, "afterImage");
        return new UpdatePayload(
                xid,
                prevLsn,
                pageNumber,
                recordOffsetInPage,
                beforeImage,
                afterImage
        );
    }

    /**
     * 编码不带业务 body 的事务状态 payload：
     * {@code [type][xid][prevLsn]}。
     */
    public static byte[] encodeTransactionState(LogRecordType type, long xid, long prevLsn) {
        Objects.requireNonNull(type, "type must not be null");
        if (!type.isTransactionStateChange()) {
            throw new IllegalArgumentException(
                    "Not a transaction state record type: " + type
            );
        }
        if (xid <= 0) {
            throw new IllegalArgumentException("xid must be positive: " + xid);
        }
        requireLsn(prevLsn, "prevLsn");
        if (type == LogRecordType.BEGIN && prevLsn != LogRecord.NO_LSN) {
            throw new IllegalArgumentException(
                    "BEGIN prevLsn must be NO_LSN: " + prevLsn
            );
        }

        ByteWriter writer = ByteWriter.allocate(COMMON_HEADER_SIZE);
        writer.writeByte(type.getCode());
        writer.writeLong(xid);
        writer.writeLong(prevLsn);
        return writer.toByteArray();
    }

    public static TransactionStatePayload decodeTransactionState(byte[] payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        ByteReader reader = ByteReader.wrap(payload);
        LogRecordType type = LogRecordType.fromCode(reader.readByte());
        if (!type.isTransactionStateChange()) {
            throw new MalformedDataException(
                    "Expected transaction state WAL record but found " + type
            );
        }
        long xid = reader.readLong();
        long prevLsn = readLsn(reader, "prevLsn");
        reader.requireFullyConsumed();

        if (xid <= 0) {
            throw new MalformedDataException("xid must be positive: " + xid);
        }
        if (type == LogRecordType.BEGIN && prevLsn != LogRecord.NO_LSN) {
            throw new MalformedDataException(
                    "BEGIN prevLsn must be NO_LSN: " + prevLsn
            );
        }
        return new TransactionStatePayload(type, xid, prevLsn);
    }

    /**
     * 编码 Compensation Log Record：
     * {@code [CLR][xid][prevLsn][undoNextLsn][pageNumber]
     * [recordOffsetInPage][compensationImageLength][compensationImage]}。
     */
    public static byte[] encodeClr(
            long xid,
            long prevLsn,
            long undoNextLsn,
            int pageNumber,
            short recordOffsetInPage,
            byte[] compensationImage
    ) {
        if (xid <= 0) {
            throw new IllegalArgumentException("xid must be positive: " + xid);
        }
        requireLsn(prevLsn, "prevLsn");
        requireLsn(undoNextLsn, "undoNextLsn");
        requirePageNumber(pageNumber);
        requireNonEmpty(compensationImage, "compensationImage");

        ByteWriter writer = ByteWriter.allocate(
                addExact(CLR_FIXED_SIZE, compensationImage.length)
        );
        writer.writeByte(LogRecordType.CLR.getCode());
        writer.writeLong(xid);
        writer.writeLong(prevLsn);
        writer.writeLong(undoNextLsn);
        writer.writeInt(pageNumber);
        writer.writeShort(recordOffsetInPage);
        writer.writeInt(compensationImage.length);
        writer.writeBytes(compensationImage);
        return writer.toByteArray();
    }

    public static ClrPayload decodeClr(byte[] payload) {
        ByteReader reader = readerFor(payload, LogRecordType.CLR);
        long xid = reader.readLong();
        long prevLsn = readLsn(reader, "prevLsn");
        long undoNextLsn = readLsn(reader, "undoNextLsn");
        int pageNumber = reader.readInt();
        short recordOffsetInPage = reader.readShort();
        int compensationImageLength =
                readLength(reader, "compensationImageLength");
        byte[] compensationImage = reader.readBytes(compensationImageLength);
        reader.requireFullyConsumed();

        if (xid <= 0) {
            throw new MalformedDataException("xid must be positive: " + xid);
        }
        requireDecodedPageNumber(pageNumber);
        requireDecodedNonEmpty(compensationImage, "compensationImage");
        return new ClrPayload(
                xid,
                prevLsn,
                undoNextLsn,
                pageNumber,
                recordOffsetInPage,
                compensationImage
        );
    }

    public static byte[] encodeBeginCheckpoint() {
        ByteWriter writer = ByteWriter.allocate(Byte.BYTES);
        writer.writeByte(LogRecordType.BEGIN_CHECKPOINT.getCode());
        return writer.toByteArray();
    }

    public static void decodeBeginCheckpoint(byte[] payload) {
        ByteReader reader = readerFor(payload, LogRecordType.BEGIN_CHECKPOINT);
        reader.requireFullyConsumed();
    }

    public static byte[] encodeCheckpointDpt(long beginCheckpointLsn, int chunkIndex, Map<Integer, Long> entries) {
        requireCheckpointChunk(beginCheckpointLsn, chunkIndex, entries);
        int payloadSize = addExact(
                CHECKPOINT_CHUNK_HEADER_SIZE,
                Math.multiplyExact(entries.size(), DPT_ENTRY_SIZE)
        );
        ByteWriter writer = ByteWriter.allocate(payloadSize);
        writer.writeByte(LogRecordType.CHECKPOINT_DPT.getCode());
        writer.writeLong(beginCheckpointLsn);
        writer.writeInt(chunkIndex);
        writer.writeInt(entries.size());
        entries.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    writer.writeInt(entry.getKey());
                    writer.writeLong(entry.getValue());
                });
        return writer.toByteArray();
    }

    public static CheckpointDptPayload decodeCheckpointDpt(byte[] payload) {
        ByteReader reader = readerFor(payload, LogRecordType.CHECKPOINT_DPT);
        long beginCheckpointLsn = readPositiveLsn(reader, "beginCheckpointLsn");
        int chunkIndex = readNonNegativeInt(reader, "chunkIndex");
        int entryCount = readNonNegativeInt(reader, "entryCount");
        requireEntryBytes(reader, entryCount, DPT_ENTRY_SIZE);

        Map<Integer, Long> entries = new LinkedHashMap<>();
        for (int index = 0; index < entryCount; index++) {
            int pageNumber = reader.readInt();
            long recoveryLsn = readPositiveLsn(reader, "recoveryLsn");
            requireDecodedPageNumber(pageNumber);
            if (entries.put(pageNumber, recoveryLsn) != null) {
                throw new MalformedDataException(
                        "Duplicate DPT pageNumber in checkpoint chunk: " + pageNumber
                );
            }
        }
        reader.requireFullyConsumed();
        return new CheckpointDptPayload(
                beginCheckpointLsn,
                chunkIndex,
                entries
        );
    }

    public static byte[] encodeCheckpointAtt(
            long beginCheckpointLsn,
            int chunkIndex,
            Map<Long, ActiveTransaction> entries
    ) {
        requireCheckpointChunk(beginCheckpointLsn, chunkIndex, entries);
        int payloadSize = addExact(
                CHECKPOINT_CHUNK_HEADER_SIZE,
                Math.multiplyExact(entries.size(), ATT_ENTRY_SIZE)
        );
        ByteWriter writer = ByteWriter.allocate(payloadSize);
        writer.writeByte(LogRecordType.CHECKPOINT_ATT.getCode());
        writer.writeLong(beginCheckpointLsn);
        writer.writeInt(chunkIndex);
        writer.writeInt(entries.size());
        entries.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    ActiveTransaction transaction =
                            entry.getValue();
                    writer.writeLong(transaction.getXid());
                    writer.writeByte(transaction.getStatus().getCode());
                    writer.writeLong(transaction.getLastLsn());
                });
        return writer.toByteArray();
    }

    public static CheckpointAttPayload decodeCheckpointAtt(byte[] payload) {
        ByteReader reader = readerFor(payload, LogRecordType.CHECKPOINT_ATT);
        long beginCheckpointLsn = readPositiveLsn(reader, "beginCheckpointLsn");
        int chunkIndex = readNonNegativeInt(reader, "chunkIndex");
        int entryCount = readNonNegativeInt(reader, "entryCount");
        requireEntryBytes(reader, entryCount, ATT_ENTRY_SIZE);

        Map<Long, ActiveTransaction> entries =
                new LinkedHashMap<>();
        for (int index = 0; index < entryCount; index++) {
            long xid = reader.readLong();
            ActiveTransaction.Status status =
                    ActiveTransaction.Status.fromCode(reader.readByte());
            long lastLsn = readPositiveLsn(reader, "lastLsn");
            ActiveTransaction transaction =
                    new ActiveTransaction(xid, status, lastLsn);
            if (entries.put(xid, transaction) != null) {
                throw new MalformedDataException(
                        "Duplicate ATT xid in checkpoint chunk: " + xid
                );
            }
        }
        reader.requireFullyConsumed();
        return new CheckpointAttPayload(
                beginCheckpointLsn,
                chunkIndex,
                entries
        );
    }

    public static byte[] encodeEndCheckpoint(long beginCheckpointLsn, int dptChunkCount, int attChunkCount) {
        if (beginCheckpointLsn <= LogRecord.NO_LSN) {
            throw new IllegalArgumentException(
                    "beginCheckpointLsn must be positive: " + beginCheckpointLsn
            );
        }
        requireNonNegative(dptChunkCount, "dptChunkCount");
        requireNonNegative(attChunkCount, "attChunkCount");

        ByteWriter writer = ByteWriter.allocate(END_CHECKPOINT_SIZE);
        writer.writeByte(LogRecordType.END_CHECKPOINT.getCode());
        writer.writeLong(beginCheckpointLsn);
        writer.writeInt(dptChunkCount);
        writer.writeInt(attChunkCount);
        return writer.toByteArray();
    }

    public static EndCheckpointPayload decodeEndCheckpoint(byte[] payload) {
        ByteReader reader = readerFor(payload, LogRecordType.END_CHECKPOINT);
        long beginCheckpointLsn = readPositiveLsn(reader, "beginCheckpointLsn");
        int dptChunkCount = readNonNegativeInt(reader, "dptChunkCount");
        int attChunkCount = readNonNegativeInt(reader, "attChunkCount");
        reader.requireFullyConsumed();
        return new EndCheckpointPayload(
                beginCheckpointLsn,
                dptChunkCount,
                attChunkCount
        );
    }

    private static ByteReader readerFor(byte[] payload, LogRecordType expectedType) {
        Objects.requireNonNull(payload, "payload must not be null");
        ByteReader reader = ByteReader.wrap(payload);
        int typePosition = reader.position();
        LogRecordType actualType = LogRecordType.fromCode(reader.readByte());
        if (actualType != expectedType) {
            throw new MalformedDataException(
                    "Expected WAL record type " + expectedType
                            + " at position " + typePosition
                            + " but found " + actualType
            );
        }
        return reader;
    }

    private static int readLength(ByteReader reader, String fieldName) {
        int fieldPosition = reader.position();
        int length = reader.readInt();
        if (length < 0) {
            throw new MalformedDataException(
                    fieldName + " must not be negative at position "
                            + fieldPosition + ": " + length
            );
        }
        return length;
    }

    private static long readLsn(ByteReader reader, String fieldName) {
        int fieldPosition = reader.position();
        long lsn = reader.readLong();
        if (lsn < LogRecord.NO_LSN) {
            throw new MalformedDataException(
                    fieldName + " must not be negative at position "
                            + fieldPosition + ": " + lsn
            );
        }
        return lsn;
    }

    private static long readPositiveLsn(ByteReader reader, String fieldName) {
        long lsn = readLsn(reader, fieldName);
        if (lsn == LogRecord.NO_LSN) {
            throw new MalformedDataException(fieldName + " must be positive");
        }
        return lsn;
    }

    private static int readNonNegativeInt(ByteReader reader, String fieldName) {
        int fieldPosition = reader.position();
        int value = reader.readInt();
        if (value < 0) {
            throw new MalformedDataException(
                    fieldName + " must not be negative at position "
                            + fieldPosition + ": " + value
            );
        }
        return value;
    }

    private static void requireEntryBytes(ByteReader reader, int entryCount, int entrySize) {
        final int expectedBytes;
        try {
            expectedBytes = Math.multiplyExact(entryCount, entrySize);
        } catch (ArithmeticException exception) {
            throw new MalformedDataException(
                    "Checkpoint entry byte count overflows",
                    exception
            );
        }
        if (reader.remaining() != expectedBytes) {
            throw new MalformedDataException(
                    "Checkpoint chunk declares " + entryCount
                            + " entries requiring " + expectedBytes
                            + " bytes but contains " + reader.remaining()
            );
        }
    }

    private static void requireLsn(long lsn, String fieldName) {
        if (lsn < LogRecord.NO_LSN) {
            throw new IllegalArgumentException(
                    fieldName + " must not be negative: " + lsn
            );
        }
    }

    private static void requireNonNegative(int value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(
                    fieldName + " must not be negative: " + value
            );
        }
    }

    private static void requireCheckpointChunk(long beginCheckpointLsn, int chunkIndex, Map<?, ?> entries) {
        if (beginCheckpointLsn <= LogRecord.NO_LSN) {
            throw new IllegalArgumentException(
                    "beginCheckpointLsn must be positive: " + beginCheckpointLsn
            );
        }
        requireNonNegative(chunkIndex, "chunkIndex");
        Objects.requireNonNull(entries, "entries must not be null");
    }

    private static void requirePageNumber(int pageNumber) {
        if (pageNumber <= 0) {
            throw new IllegalArgumentException("pageNumber must be positive: " + pageNumber);
        }
    }

    private static void requireDecodedPageNumber(int pageNumber) {
        if (pageNumber <= 0) {
            throw new MalformedDataException(
                    "pageNumber must be positive: " + pageNumber
            );
        }
    }

    private static void requireNonEmpty(byte[] bytes, String fieldName) {
        Objects.requireNonNull(bytes, fieldName + " must not be null");
        if (bytes.length == 0) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
    }

    private static void requireDecodedNonEmpty(byte[] bytes, String fieldName) {
        if (bytes.length == 0) {
            throw new MalformedDataException(fieldName + " must not be empty");
        }
    }

    private static int addExact(int left, int right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("WAL payload is too large", exception);
        }
    }

    public static final class InsertPayload {
        private final long xid;
        private final long prevLsn;
        private final int pageNumber;
        private final short recordOffsetInPage;
        private final byte[] recordBytes;

        private InsertPayload(long xid, long prevLsn, int pageNumber, short recordOffsetInPage, byte[] recordBytes) {
            this.xid = xid;
            this.prevLsn = prevLsn;
            this.pageNumber = pageNumber;
            this.recordOffsetInPage = recordOffsetInPage;
            this.recordBytes = recordBytes;
        }

        public long getXid() {
            return xid;
        }

        public long getPrevLsn() {
            return prevLsn;
        }

        public int getPageNumber() {
            return pageNumber;
        }

        public short getRecordOffsetInPage() {
            return recordOffsetInPage;
        }

        public byte[] getRecordBytes() {
            return Arrays.copyOf(recordBytes, recordBytes.length);
        }
    }

    public static final class UpdatePayload {
        private final long xid;
        private final long prevLsn;
        private final int pageNumber;
        private final short recordOffsetInPage;
        private final byte[] beforeImage;
        private final byte[] afterImage;

        private UpdatePayload(
                long xid,
                long prevLsn,
                int pageNumber,
                short recordOffsetInPage,
                byte[] beforeImage,
                byte[] afterImage
        ) {
            this.xid = xid;
            this.prevLsn = prevLsn;
            this.pageNumber = pageNumber;
            this.recordOffsetInPage = recordOffsetInPage;
            this.beforeImage = beforeImage;
            this.afterImage = afterImage;
        }

        public long getXid() {
            return xid;
        }

        public long getPrevLsn() {
            return prevLsn;
        }

        public int getPageNumber() {
            return pageNumber;
        }

        public short getRecordOffsetInPage() {
            return recordOffsetInPage;
        }

        public byte[] getBeforeImage() {
            return Arrays.copyOf(beforeImage, beforeImage.length);
        }

        public byte[] getAfterImage() {
            return Arrays.copyOf(afterImage, afterImage.length);
        }
    }

    public static final class TransactionStatePayload {
        private final LogRecordType type;
        private final long xid;
        private final long prevLsn;

        private TransactionStatePayload(LogRecordType type, long xid, long prevLsn) {
            this.type = type;
            this.xid = xid;
            this.prevLsn = prevLsn;
        }

        public LogRecordType getType() {
            return type;
        }

        public long getXid() {
            return xid;
        }

        public long getPrevLsn() {
            return prevLsn;
        }
    }

    public static final class ClrPayload {
        private final long xid;
        private final long prevLsn;
        private final long undoNextLsn;
        private final int pageNumber;
        private final short recordOffsetInPage;
        private final byte[] compensationImage;

        private ClrPayload(
                long xid,
                long prevLsn,
                long undoNextLsn,
                int pageNumber,
                short recordOffsetInPage,
                byte[] compensationImage
        ) {
            this.xid = xid;
            this.prevLsn = prevLsn;
            this.undoNextLsn = undoNextLsn;
            this.pageNumber = pageNumber;
            this.recordOffsetInPage = recordOffsetInPage;
            this.compensationImage = compensationImage;
        }

        public long getXid() {
            return xid;
        }

        public long getPrevLsn() {
            return prevLsn;
        }

        public long getUndoNextLsn() {
            return undoNextLsn;
        }

        public int getPageNumber() {
            return pageNumber;
        }

        public short getRecordOffsetInPage() {
            return recordOffsetInPage;
        }

        public byte[] getCompensationImage() {
            return Arrays.copyOf(compensationImage, compensationImage.length);
        }
    }

    public static final class CheckpointDptPayload {
        private final long beginCheckpointLsn;
        private final int chunkIndex;
        private final Map<Integer, Long> entries;

        private CheckpointDptPayload(long beginCheckpointLsn, int chunkIndex, Map<Integer, Long> entries) {
            this.beginCheckpointLsn = beginCheckpointLsn;
            this.chunkIndex = chunkIndex;
            this.entries = Collections.unmodifiableMap(new LinkedHashMap<>(entries));
        }

        public long getBeginCheckpointLsn() {
            return beginCheckpointLsn;
        }

        public int getChunkIndex() {
            return chunkIndex;
        }

        public Map<Integer, Long> getEntries() {
            return entries;
        }
    }

    public static final class CheckpointAttPayload {
        private final long beginCheckpointLsn;
        private final int chunkIndex;
        private final Map<Long, ActiveTransaction> entries;

        private CheckpointAttPayload(long beginCheckpointLsn, int chunkIndex, Map<Long, ActiveTransaction> entries) {
            this.beginCheckpointLsn = beginCheckpointLsn;
            this.chunkIndex = chunkIndex;
            this.entries = Collections.unmodifiableMap(new LinkedHashMap<>(entries));
        }

        public long getBeginCheckpointLsn() {
            return beginCheckpointLsn;
        }

        public int getChunkIndex() {
            return chunkIndex;
        }

        public Map<Long, ActiveTransaction> getEntries() {
            return entries;
        }
    }

    public static final class EndCheckpointPayload {
        private final long beginCheckpointLsn;
        private final int dptChunkCount;
        private final int attChunkCount;

        private EndCheckpointPayload(long beginCheckpointLsn, int dptChunkCount, int attChunkCount) {
            this.beginCheckpointLsn = beginCheckpointLsn;
            this.dptChunkCount = dptChunkCount;
            this.attChunkCount = attChunkCount;
        }

        public long getBeginCheckpointLsn() {
            return beginCheckpointLsn;
        }

        public int getDptChunkCount() {
            return dptChunkCount;
        }

        public int getAttChunkCount() {
            return attChunkCount;
        }
    }
}
