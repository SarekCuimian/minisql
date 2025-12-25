package com.minisql.backend.vm;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import com.minisql.backend.common.SubArray;
import com.minisql.backend.dm.dataitem.DataItem;
import com.minisql.backend.utils.ByteUtil;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [getData]
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * entry结构：
     * [XMIN] [XMAX] [getData]
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = ByteUtil.longToByte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void releaseDataItem() {
        dataItem.release();
    }

    // 以拷贝的形式返回内容
    public byte[] getData() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 覆盖写入数据区（保持 xmin/xmax 不变）。
     * 仅支持新数据长度与原数据长度一致的场景。
     */
    public void setData(byte[] data, long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            int bodyLen = sa.end - (sa.start + OF_DATA);
            if(bodyLen != data.length) {
                throw new IllegalArgumentException("overwrite length mismatch");
            }
            System.arraycopy(data, 0, sa.raw, sa.start + OF_DATA, data.length);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return ByteUtil.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return ByteUtil.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(ByteUtil.longToByte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }

}
