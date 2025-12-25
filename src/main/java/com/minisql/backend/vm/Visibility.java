package com.minisql.backend.vm;

import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.txm.TransactionManagerImpl;

public class Visibility {
    
    public static boolean isVersionSkip(TransactionManager txm, Transaction tx, Entry e) {
        long xmax = e.getXmax();
        if(tx.level == IsolationLevel.READ_COMMITTED) {
            return false;
        } else {
            return txm.isCommitted(xmax) && (xmax > tx.xid || tx.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager txm, Transaction tx, Entry e) {
        // 超级事务用于元信息加载，不受可见性规则约束
        if(tx.xid == TransactionManagerImpl.SUPER_XID) {
            return true;
        }
        if(tx.level == IsolationLevel.READ_COMMITTED) {
            return readCommitted(txm, tx, e);
        } else {
            return repeatableRead(txm, tx, e);
        }
    }

    private static boolean readCommitted(TransactionManager txm, Transaction tx, Entry e) {
        long xid = tx.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(txm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!txm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager txm, Transaction tx, Entry e) {
        long xid = tx.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(txm.isCommitted(xmin) && xmin < xid && !tx.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!txm.isCommitted(xmax) || xmax > xid || tx.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
