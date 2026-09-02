package com.minisql.engine.transaction.mvcc;

import com.minisql.engine.transaction.xid.XidAllocator;

/** 当前进程中一次正在执行的事务。 */
public final class RuntimeTransaction {
    public long xid;
    public IsolationLevel level;
    /** 只读事务不会写入 XID 文件或 WAL，也不能进入任何修改路径。 */
    public boolean readOnly;
    /** REPEATABLE READ 事务复用的事务级视图；READ COMMITTED 为 null。 */
    public ReadView readView;
    public Exception error;
    public boolean autoAborted;
    /**
     * 运行期事务状态。COMMITTING/ABORTING 事务仍保留在活跃事务表中，
     * 使新建 RR 快照不会漏掉尚未完成最终状态转换的事务。
     */
    public volatile State state;

    private RuntimeTransaction() {}

    public static RuntimeTransaction newTransaction(long xid, IsolationLevel level, ReadView readView) {
        RuntimeTransaction tx = new RuntimeTransaction();
        tx.xid = xid;
        tx.level = level;
        tx.readOnly = false;
        tx.state = State.ACTIVE;
        tx.readView = readView;
        if (level == IsolationLevel.REPEATABLE_READ && readView == null) {
            throw new IllegalArgumentException(
                    "REPEATABLE READ transaction requires a ReadView"
            );
        }
        if (level == IsolationLevel.READ_COMMITTED && readView != null) {
            throw new IllegalArgumentException(
                    "READ COMMITTED transaction must not retain a ReadView"
            );
        }
        return tx;
    }

    public static RuntimeTransaction newReadOnlyTransaction(long xid) {
        if (xid >= XidAllocator.SYSTEM_XID) {
            throw new IllegalArgumentException(
                    "read-only xid must be negative: " + xid
            );
        }
        RuntimeTransaction tx = new RuntimeTransaction();
        tx.xid = xid;
        tx.level = IsolationLevel.READ_COMMITTED;
        tx.readOnly = true;
        tx.state = State.ACTIVE;
        return tx;
    }

    public boolean isActive() {
        return state == State.ACTIVE;
    }

    public enum State {
        ACTIVE,
        COMMITTING,
        ABORTING,
        COMMITTED,
        ABORTED
    }

}
