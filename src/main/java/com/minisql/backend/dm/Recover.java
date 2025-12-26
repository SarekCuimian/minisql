package com.minisql.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import com.minisql.backend.common.SubArray;
import com.minisql.backend.dm.dataitem.DataItem;
import com.minisql.backend.dm.logger.LogManager;
import com.minisql.backend.dm.page.Page;
import com.minisql.backend.dm.page.PageX;
import com.minisql.backend.dm.page.cache.PageCache;
import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.ByteUtil;

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    // UpdateLog: [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    // InsertLog: [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;
    
    private enum Mode {
        REDO, UNDO
    }

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager txm, LogManager lgm, PageCache pc) {
        System.out.println("Recovering...");

        int maxPgno = 0;
        try (LogManager.LogReader reader = lgm.getReader()) {
            while(true) {
                byte[] log = reader.next();
                if(log == null) break;
                int pgno;
                if(isInsertLog(log)) {
                    InsertLogInfo li = parseInsertLog(log);
                    pgno = li.pgno;
                } else {
                    UpdateLogInfo li = parseUpdateLog(log);
                    pgno = li.pgno;
                }
                if(pgno > maxPgno) {
                    maxPgno = pgno;
                }
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        try (LogManager.LogReader reader = lgm.getReader()) {
            redoTransactions(txm, reader, pc);
        }
        System.out.println("Redo Transactions Over.");

        try (LogManager.LogReader reader = lgm.getReader()) {
            undoTransactions(txm, reader, pc);
        }
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    private static void redoTransactions(TransactionManager txm, LogManager.LogReader reader, PageCache pc) {
        while(true) {
            byte[] log = reader.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!txm.isActive(xid)) {
                    doInsertLog(pc, log, Mode.REDO);
                }
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                long xid = li.xid;
                if(!txm.isActive(xid)) {
                    doUpdateLog(pc, log, Mode.REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager txm, LogManager.LogReader reader, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        while(true) {
            byte[] log = reader.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(txm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                long xid = li.xid;
                if(txm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有 active log 进行倒序 undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, Mode.UNDO);
                } else {
                    doUpdateLog(pc, log, Mode.UNDO);
                }
            }
            txm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = ByteUtil.longToByte(xid);
        byte[] uidRaw = ByteUtil.longToByte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = ByteUtil.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = ByteUtil.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, Mode mode) {
        int pgno;
        short offset;
        byte[] raw;
        if(mode == Mode.REDO) {
            UpdateLogInfo li = parseUpdateLog(log);
            pgno = li.pgno;
            offset = li.offset;
            raw = li.newRaw;
        } else {
            UpdateLogInfo li = parseUpdateLog(log);
            pgno = li.pgno;
            offset = li.offset;
            raw = li.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.of(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }


    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = ByteUtil.longToByte(xid);
        byte[] pgnoRaw = ByteUtil.intToByte(pg.getPageNumber());
        byte[] offsetRaw = ByteUtil.shortToByte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo logInfo = new InsertLogInfo();
        logInfo.xid = ByteUtil.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        logInfo.pgno = ByteUtil.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        logInfo.offset = ByteUtil.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        logInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return logInfo;
    }

    private static void doInsertLog(PageCache pc, byte[] log, Mode mode) {
        InsertLogInfo logInfo = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(logInfo.pgno);
        } catch(Exception e) {
            Panic.of(e);
        }
        try {
            if(mode == Mode.UNDO) {
                DataItem.setDataItemRawInvalid(logInfo.raw);
            }
            PageX.recoverInsert(pg, logInfo.raw, logInfo.offset);
        } finally {
            pg.release();
        }
    }
}
