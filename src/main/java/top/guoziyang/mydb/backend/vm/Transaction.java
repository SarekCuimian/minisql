package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    public long xid;
    public IsolationLevel level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, IsolationLevel level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level == IsolationLevel.REPEATABLE_READ && active != null) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(snapshot == null) {
            return false;
        }
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
