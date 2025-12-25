package com.minisql.backend.vm;

import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.txm.TransactionManagerImpl;

public class Visibility {
    
    public static boolean isVersionSkip(TransactionManager txm, Transaction tx, Entry e) {
        long xmax = e.getXmax();
        if(tx.level == IsolationLevel.READ_COMMITTED) {
            // RC 读已提交，即其他后来的事务删除了这条记录且已经提交，当前 RC 下的事务能看到这条记录被删除
            return false;
        } else {
            // RR 下扫描版本链时，如果该版本的 xmax 已提交 且 (属于开启得更晚的事务 或 快照中依然活跃的事务)
            // 也就是最新版本相比快照已经发生了改变，基于快照更新不再有意义
            // 则对当前事务来此版本的记录已经无效，不应该再基于这个版本进行删除/更新
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
        // 自己写入且未删除，可见
        if(xmin == xid && xmax == 0) return true;
        // 其他事务写入而且 xmin 已提交
        if(txm.isCommitted(xmin)) {
            // 未被删除，可见
            if(xmax == 0) return true;
            if(xmax != xid) {
                // 被标记了删除，如果 xmax 事务未提交，则可见
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
        // 自己写入且未删除可见
        if(xmin == xid && xmax == 0) return true;
        // 其他事务写入需 xmin 已提交，且小于当前 xid，且不在当前激活事务快照中
        if(txm.isCommitted(xmin) && xmin < xid && !tx.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                // 被标记了删除，需 xmax 事务未提交
                // 或 xmax 事务在当前事务之后开启
                // 或 xmax 在激活事务快照中
                if(!txm.isCommitted(xmax) || xmax > xid || tx.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
