package com.minisql.engine.transaction.mvcc;

import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.transaction.xid.XidStatusTable;

public class Visibility {
    
    private Visibility() {
    }

    public static boolean isVersionConflict(XidStatusTable xidStatusTable, ReadView readView, Entry entry) {
        long deletingXid = entry.getXmax();
        return deletingXid != XidAllocator.SYSTEM_XID
                && xidStatusTable.isCommitted(deletingXid)
                && !readView.isTransactionVisible(
                        deletingXid,
                        xidStatusTable
                );
    }

    public static boolean isVisible(XidStatusTable xidStatusTable, ReadView readView, Entry entry) {
        if (readView.getOwnerXid() == XidAllocator.SYSTEM_XID) {
            return true;
        }
        if (!readView.isTransactionVisible(
                entry.getXmin(),
                xidStatusTable
        )) {
            return false;
        }

        long deletingXid = entry.getXmax();
        if (deletingXid == XidAllocator.SYSTEM_XID) {
            return true;
        }
        return !readView.isTransactionVisible(
                deletingXid,
                xidStatusTable
        );
    }
}
