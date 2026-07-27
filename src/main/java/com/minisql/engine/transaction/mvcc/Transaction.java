package com.minisql.engine.transaction.mvcc;

import java.util.HashMap;
import java.util.Map;

import com.minisql.engine.transaction.status.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    public long xid;
    public IsolationLevel level;
    /** 只读事务不会写入 XID 文件或 WAL，也不能进入任何修改路径。 */
    public boolean readOnly;
    /** 事务开始时仍然活跃的其他事务 ID */
    public Map<Long, Boolean> activeSnapshot;
    public Exception error;
    public boolean autoAborted;
    public volatile boolean terminated = false;  // 新增：事务终止标志

    private Transaction() {}

    public static Transaction newTransaction(long xid, IsolationLevel level, Map<Long, Transaction> active) {
        Transaction tx = new Transaction();
        tx.xid = xid;
        tx.level = level;
        tx.readOnly = false;
        if(level == IsolationLevel.REPEATABLE_READ && active != null) {
            tx.activeSnapshot = new HashMap<>();
            for(Transaction transaction : active.values()) {
                // SUPER_XID 与进程内只读 XID 都不会出现在持久化记录中，无需进入快照。
                if (transaction.xid > TransactionManagerImpl.SUPER_XID) {
                    tx.activeSnapshot.put(transaction.xid, true);
                }
            }
        }
        return tx;
    }

    public static Transaction newReadOnlyTransaction(long xid) {
        if (xid >= TransactionManagerImpl.SUPER_XID) {
            throw new IllegalArgumentException(
                    "read-only xid must be negative: " + xid
            );
        }
        Transaction tx = new Transaction();
        tx.xid = xid;
        tx.level = IsolationLevel.READ_COMMITTED;
        tx.readOnly = true;
        return tx;
    }

    /**
     * 判断事务是否在激活事务快照中
     * @param xid 事务ID
     */
    public boolean isInSnapshot(long xid) {
        if(activeSnapshot == null) {
            return false;
        }
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return activeSnapshot.containsKey(xid);
    }
}
