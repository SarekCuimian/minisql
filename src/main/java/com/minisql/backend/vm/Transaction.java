package com.minisql.backend.vm;

import java.util.HashMap;
import java.util.Map;

import com.minisql.backend.txm.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    public long xid;
    public IsolationLevel level;
    /**
     * 事务开始时仍然活跃的其他事务 ID
     */
    public Map<Long, Boolean> activeSnapshot;
    public Exception err;
    public boolean autoAborted;

    private Transaction() {}

    public static Transaction newTransaction(long xid, IsolationLevel level, Map<Long, Transaction> active) {
        Transaction tx = new Transaction();
        tx.xid = xid;
        tx.level = level;
        if(level == IsolationLevel.REPEATABLE_READ && active != null) {
            tx.activeSnapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                tx.activeSnapshot.put(x, true);
            }
        }
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
