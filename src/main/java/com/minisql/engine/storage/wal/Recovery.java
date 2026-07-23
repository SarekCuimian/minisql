package com.minisql.engine.storage.wal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.codec.ByteUtil;
import com.minisql.engine.storage.codec.UidUtil;
import com.minisql.engine.storage.page.DataPage;
import com.minisql.engine.storage.page.Page;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.storage.record.PageRecord;
import com.minisql.engine.transaction.status.TransactionManager;
import com.minisql.error.Panic;

/**
 * 基于 WAL 执行 REDO 与 UNDO 的恢复协调器。
 *
 * <p>Update log 的布局为：
 * {@code [LogType][XID][UID][BeforeImage][AfterImage]}。</p>
 *
 * <p>Insert log 的布局为：
 * {@code [LogType][XID][PGNO][RecordOffset][RecordBytes]}。</p>
 */
public final class Recovery {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int LOG_TYPE_OFFSET = 0;
    private static final int XID_OFFSET = LOG_TYPE_OFFSET + 1;
    private static final int UPDATE_UID_OFFSET = XID_OFFSET + Long.BYTES;
    /** Update log 中 before image 与 after image 的公共起点。 */
    private static final int UPDATE_IMAGE_OFFSET = UPDATE_UID_OFFSET + Long.BYTES;

    private static final int INSERT_PGNO_OFFSET = XID_OFFSET + Long.BYTES;
    /** Insert log 中记录页内位置字段的字节偏移。 */
    private static final int INSERT_POSITION_OFFSET = INSERT_PGNO_OFFSET + Integer.BYTES;
    /** Insert log 中完整 Record bytes 的起始位置。 */
    private static final int INSERT_RECORD_OFFSET = INSERT_POSITION_OFFSET + Short.BYTES;

    private Recovery() {
    }

    private enum RecoveryMode {
        REDO,
        UNDO
    }

    /**
     * 解析后的 Insert log。
     * recordBytes 是完整 physical record bytes，包含 Record header 与 payload。
     */
    private static final class InsertLogRecord {
        /** 产生该日志的事务 XID。 */
        final long xid;
        /** 写入目标的 PGNO。 */
        final int pgno;
        /** Record 在 Page 中的起始位置。 */
        final short recordOffset;
        /** Insert 的完整 Record bytes；UNDO 时会将其标记为无效。 */
        final byte[] recordBytes;

        private InsertLogRecord(long xid, int pgno, short recordOffset, byte[] recordBytes) {
            this.xid = xid;
            this.pgno = pgno;
            this.recordOffset = recordOffset;
            this.recordBytes = recordBytes;
        }
    }

    /**
     * 解析后的 Update log。
     * beforeImage 用于 UNDO，afterImage 用于 REDO，二者均为完整 physical record bytes。
     */
    private static final class UpdateLogRecord {
        /** 产生该日志的事务 XID。 */
        final long xid;
        /** 被更新 Record 所在的 PGNO。 */
        final int pgno;
        /** Record 在 Page 中的起始位置。 */
        final short recordOffset;
        /** 更新前的完整 Record bytes，用于 UNDO。 */
        final byte[] beforeImage;
        /** 更新后的完整 Record bytes，用于 REDO。 */
        final byte[] afterImage;

        private UpdateLogRecord(
                long xid,
                int pgno,
                short recordOffset,
                byte[] beforeImage,
                byte[] afterImage
        ) {
            this.xid = xid;
            this.pgno = pgno;
            this.recordOffset = recordOffset;
            this.beforeImage = beforeImage;
            this.afterImage = afterImage;
        }
    }

    /** 执行恢复：裁剪无效页尾、REDO 非 active 事务，再 UNDO active 事务。 */
    public static void recover(
            TransactionManager transactionManager,
            LogManager logManager,
            PageCache pageCache
    ) {
        System.out.println("Recovering...");

        int maxPgno = findMaxLoggedPgno(logManager);
        pageCache.trimBadTail(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        try (LogManager.LogReader reader = logManager.getReader()) {
            redoNonActiveTransactions(transactionManager, reader, pageCache);
        }
        System.out.println("Redo Transactions Over.");

        try (LogManager.LogReader reader = logManager.getReader()) {
            undoActiveTransactions(transactionManager, reader, pageCache);
        }
        System.out.println("Undo Transactions Over.");
        System.out.println("Recovery Over.");
    }

    private static int findMaxLoggedPgno(LogManager logManager) {
        int maxPgno = 0;
        try (LogManager.LogReader reader = logManager.getReader()) {
            while (true) {
                byte[] logBytes = reader.next();
                if (logBytes == null) {
                    break;
                }

                int pgno = isInsertLog(logBytes)
                        ? parseInsertLog(logBytes).pgno
                        : parseUpdateLog(logBytes).pgno;
                maxPgno = Math.max(maxPgno, pgno);
            }
        }
        return maxPgno == 0 ? 1 : maxPgno;
    }

    /** 对崩溃时已非 active 的事务重放所有 WAL changes。 */
    private static void redoNonActiveTransactions(
            TransactionManager transactionManager,
            LogManager.LogReader reader,
            PageCache pageCache
    ) {
        while (true) {
            byte[] logBytes = reader.next();
            if (logBytes == null) {
                break;
            }

            long xid = isInsertLog(logBytes)
                    ? parseInsertLog(logBytes).xid
                    : parseUpdateLog(logBytes).xid;
            if (transactionManager.isActive(xid)) {
                continue;
            }

            if (isInsertLog(logBytes)) {
                replayInsertLog(pageCache, logBytes, RecoveryMode.REDO);
            } else {
                replayUpdateLog(pageCache, logBytes, RecoveryMode.REDO);
            }
        }
    }

    /** 收集崩溃时仍 active 的事务日志，并以逆序执行 UNDO。 */
    private static void undoActiveTransactions(
            TransactionManager transactionManager,
            LogManager.LogReader reader,
            PageCache pageCache
    ) {
        Map<Long, List<byte[]>> logsByXid = new HashMap<>();
        while (true) {
            byte[] logBytes = reader.next();
            if (logBytes == null) {
                break;
            }

            long xid = isInsertLog(logBytes)
                    ? parseInsertLog(logBytes).xid
                    : parseUpdateLog(logBytes).xid;
            if (transactionManager.isActive(xid)) {
                logsByXid.computeIfAbsent(xid, ignored -> new ArrayList<>()).add(logBytes);
            }
        }

        for (Map.Entry<Long, List<byte[]>> transactionLogs : logsByXid.entrySet()) {
            List<byte[]> logBytesList = transactionLogs.getValue();
            for (int index = logBytesList.size() - 1; index >= 0; index--) {
                byte[] logBytes = logBytesList.get(index);
                if (isInsertLog(logBytes)) {
                    replayInsertLog(pageCache, logBytes, RecoveryMode.UNDO);
                } else {
                    replayUpdateLog(pageCache, logBytes, RecoveryMode.UNDO);
                }
            }
            transactionManager.abort(transactionLogs.getKey());
        }
    }

    /** 根据更新前后的 Record image 创建 Update WAL bytes。 */
    public static byte[] newUpdateLogBytes(long xid, PageRecord record) {
        byte[] beforeImage = record.getBeforeImage();
        ByteSlice recordView = record.recordView();
        byte[] afterImage = Arrays.copyOfRange(
                recordView.bytes(),
                recordView.offset(),
                recordView.end()
        );

        byte[] logBytes = new byte[UPDATE_IMAGE_OFFSET + beforeImage.length + afterImage.length];
        logBytes[LOG_TYPE_OFFSET] = LOG_TYPE_UPDATE;
        ByteUtil.putLong(logBytes, XID_OFFSET, xid);
        ByteUtil.putLong(logBytes, UPDATE_UID_OFFSET, record.getUid());
        System.arraycopy(beforeImage, 0, logBytes, UPDATE_IMAGE_OFFSET, beforeImage.length);
        System.arraycopy(afterImage, 0, logBytes, UPDATE_IMAGE_OFFSET + beforeImage.length, afterImage.length);
        return logBytes;
    }

    /** 根据页内写入位置与完整 Record bytes 创建 Insert WAL bytes。 */
    public static byte[] newInsertLogBytes(long xid, Page page, byte[] recordBytes) {
        byte[] logBytes = new byte[INSERT_RECORD_OFFSET + recordBytes.length];
        logBytes[LOG_TYPE_OFFSET] = LOG_TYPE_INSERT;
        ByteUtil.putLong(logBytes, XID_OFFSET, xid);
        ByteUtil.putInt(logBytes, INSERT_PGNO_OFFSET, page.getPageNumber());
        ByteUtil.putShort(logBytes, INSERT_POSITION_OFFSET, DataPage.getFso(page));
        System.arraycopy(recordBytes, 0, logBytes, INSERT_RECORD_OFFSET, recordBytes.length);
        return logBytes;
    }

    private static boolean isInsertLog(byte[] logBytes) {
        return logBytes[LOG_TYPE_OFFSET] == LOG_TYPE_INSERT;
    }

    private static UpdateLogRecord parseUpdateLog(byte[] logBytes) {
        long xid = ByteUtil.getLong(logBytes, XID_OFFSET);
        long uid = ByteUtil.getLong(logBytes, UPDATE_UID_OFFSET);
        short recordOffset = UidUtil.getOffset(uid);
        int pgno = UidUtil.getPgno(uid);
        int imageLength = (logBytes.length - UPDATE_IMAGE_OFFSET) / 2;
        byte[] beforeImage = Arrays.copyOfRange(
                logBytes,
                UPDATE_IMAGE_OFFSET,
                UPDATE_IMAGE_OFFSET + imageLength
        );
        byte[] afterImage = Arrays.copyOfRange(
                logBytes,
                UPDATE_IMAGE_OFFSET + imageLength,
                UPDATE_IMAGE_OFFSET + imageLength * 2
        );
        return new UpdateLogRecord(xid, pgno, recordOffset, beforeImage, afterImage);
    }

    private static InsertLogRecord parseInsertLog(byte[] logBytes) {
        long xid = ByteUtil.getLong(logBytes, XID_OFFSET);
        int pgno = ByteUtil.getInt(logBytes, INSERT_PGNO_OFFSET);
        short recordOffset = ByteUtil.getShort(logBytes, INSERT_POSITION_OFFSET);
        byte[] recordBytes = Arrays.copyOfRange(logBytes, INSERT_RECORD_OFFSET, logBytes.length);
        return new InsertLogRecord(xid, pgno, recordOffset, recordBytes);
    }

    private static void replayUpdateLog(PageCache pageCache, byte[] logBytes, RecoveryMode mode) {
        UpdateLogRecord logRecord = parseUpdateLog(logBytes);
        byte[] recordBytes = mode == RecoveryMode.REDO
                ? logRecord.afterImage
                : logRecord.beforeImage;

        Page page;
        try {
            page = pageCache.getPage(logRecord.pgno);
        } catch (Exception exception) {
            Panic.of(exception);
            return;
        }
        try {
            DataPage.recoverUpdate(page, recordBytes, logRecord.recordOffset);
        } finally {
            page.release();
        }
    }

    private static void replayInsertLog(PageCache pageCache, byte[] logBytes, RecoveryMode mode) {
        InsertLogRecord logRecord = parseInsertLog(logBytes);
        Page page;
        try {
            page = pageCache.getPage(logRecord.pgno);
        } catch (Exception exception) {
            Panic.of(exception);
            return;
        }
        try {
            if (mode == RecoveryMode.UNDO) {
                PageRecord.markInvalid(logRecord.recordBytes);
            }
            DataPage.recoverInsert(page, logRecord.recordBytes, logRecord.recordOffset);
        } finally {
            page.release();
        }
    }
}
