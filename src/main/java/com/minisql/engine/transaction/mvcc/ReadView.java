package com.minisql.engine.transaction.mvcc;

import java.util.Objects;
import java.util.Set;

import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.transaction.xid.XidStatusTable;

/**
 * 一致性读使用的不可变事务快照。
 *
 * <p>{@code futureXidBoundary} 是创建视图时下一个尚未分配的持久化 XID；
 * {@code activeXids} 保存当时尚未完成的持久化事务。二者共同区分
 * “快照前已提交”“快照时仍活跃”和“快照后才开始”的事务。</p>
 */
public final class ReadView {

    private final long ownerXid;
    private final long futureXidBoundary;
    private final Set<Long> activeXids;

    public ReadView(long ownerXid, long futureXidBoundary, Set<Long> activeXids) {
        if (futureXidBoundary <= XidAllocator.SYSTEM_XID) {
            throw new IllegalArgumentException(
                    "futureXidBoundary must be positive: "
                            + futureXidBoundary
            );
        }
        this.ownerXid = ownerXid;
        this.futureXidBoundary = futureXidBoundary;
        this.activeXids = Set.copyOf(
                Objects.requireNonNull(
                        activeXids,
                        "activeXids must not be null"
                )
        );
    }

    public static ReadView system(long futureXidBoundary) {
        return new ReadView(
                XidAllocator.SYSTEM_XID,
                futureXidBoundary,
                Set.of()
        );
    }

    public long getOwnerXid() {
        return ownerXid;
    }

    public long getFutureXidBoundary() {
        return futureXidBoundary;
    }

    public Set<Long> getActiveXids() {
        return activeXids;
    }

    /**
     * 判断目标事务的修改是否属于当前一致性视图。
     */
    public boolean isTransactionVisible(long xid, XidStatusTable xidStatusTable) {
        Objects.requireNonNull(
                xidStatusTable,
                "xidStatusTable must not be null"
        );

        // System 读取用于加载数据库元数据，保持原有“不受 MVCC 限制”的语义。
        if (ownerXid == XidAllocator.SYSTEM_XID) {
            return true;
        }
        if (xid == XidAllocator.SYSTEM_XID || xid == ownerXid) {
            return true;
        }
        if (xid >= futureXidBoundary || activeXids.contains(xid)) {
            return false;
        }
        return xidStatusTable.isCommitted(xid);
    }
}
