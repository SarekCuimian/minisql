package com.minisql.engine.index;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.storage.record.PageRecord;
import com.minisql.engine.index.Node.ChildLookupResult;
import com.minisql.engine.index.Node.InsertResult;
import com.minisql.engine.index.Node.RangeSearchResult;
import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.storage.codec.ByteUtil;

public class BPlusTree {
    PageRecordManager pageRecordManager;
    long bootUid;
    PageRecord bootRecord;
    Lock bootLock;

    public static long create(PageRecordManager pageRecordManager) throws Exception {
        byte[] rootBytes = Node.newEmptyRootBytes();
        long rootUid = pageRecordManager.insert(XidAllocator.SYSTEM_XID, rootBytes);
        byte[] rootPointer = new byte[Long.BYTES];
        ByteUtil.putLong(rootPointer, 0, rootUid);
        return pageRecordManager.insert(XidAllocator.SYSTEM_XID, rootPointer);
    }

    public static BPlusTree load(long bootUid, PageRecordManager pageRecordManager) throws Exception {
        PageRecord bootRecord = pageRecordManager.acquire(bootUid);
        assert bootRecord != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.pageRecordManager = pageRecordManager;
        t.bootRecord = bootRecord;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            ByteSlice payload = bootRecord.payload();
            return ByteUtil.getLong(payload.bytes(), payload.offset());
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long leftUid, long rightUid, long separatorKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootBytes = Node.newRootBytes(leftUid, rightUid, separatorKey);
            long newRootUid = pageRecordManager.insert(XidAllocator.SYSTEM_XID, rootBytes);
            bootRecord.startUpdate();
            ByteSlice payload = bootRecord.payload();
            ByteUtil.putLong(payload.bytes(), payload.offset(), newRootUid);
            bootRecord.finishUpdate(XidAllocator.SYSTEM_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 从任意一个节点 uid 出发，沿着 B+ 树往下找到包含指定 key 的叶子节点 uid
     * @param nodeUid 任意一个非叶子节点
     * @param key 要查找的 key
     * @return 叶子节点的 uid
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        boolean isLeaf;
        try (Node node = Node.load(this, nodeUid)) {
            isLeaf = node.isLeaf();
        }

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = lookupChild(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 找到第一个大于等于 key 的节点 uid
     * @param nodeUid 任意一个非叶子节点
     * @param key 要查找的 key
     * @return 找到的节点 uid
     */
    private long lookupChild(long nodeUid, long key) throws Exception {
        while(true) {
            ChildLookupResult result;
            try (Node node = Node.load(this, nodeUid)) {
                result = node.lookupChild(key);
            }
            if(result.childUid != 0) return result.childUid;
            nodeUid = result.siblingUid;
        }
    }

    /**
     * 查找值等等于 key 的所有 uid
     * @param key 要查找的 key
     * @return 找到的节点 uid
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 找到所有 key 的范围在 [leftKey, rightKey] 的 uid
     * @param leftKey 范围左边界
     * @param rightKey 范围右边界
     * @return 找到的节点 uid
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            RangeSearchResult result;
            try (Node leaf = Node.load(this, leafUid)) {
                result = leaf.searchRange(leftKey, rightKey);
            }
            uids.addAll(result.uids);
            if(result.siblingUid == 0) {
                break;
            } else {
                leafUid = result.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        SplitPropagationResult result = insert(rootUid, uid, key);
        if(result.rightNodeUid != 0) {
            updateRootUid(rootUid, result.rightNodeUid, result.separatorKey);
        }
    }

    private SplitPropagationResult insert(long nodeUid, long uid, long key) throws Exception {
        boolean isLeaf;
        try (Node node = Node.load(this, nodeUid)) {
            isLeaf = node.isLeaf();
        }

        if(isLeaf) {
            return insertAndSplit(nodeUid, uid, key);
        }

        long childUid = lookupChild(nodeUid, key);
        SplitPropagationResult childResult = insert(childUid, uid, key);
        if(childResult.rightNodeUid == 0) {
            return SplitPropagationResult.noSplit();
        }
        return insertAndSplit(
                nodeUid,
                childResult.rightNodeUid,
                childResult.separatorKey
        );
    }

    private SplitPropagationResult insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            InsertResult result;
            try (Node node = Node.load(this, nodeUid)) {
                result = node.insertAndSplit(uid, key);
            }
            if(result.siblingUid != 0) {
                nodeUid = result.siblingUid;
                continue;
            }
            return result.rightNodeUid == 0
                    ? SplitPropagationResult.noSplit()
                    : SplitPropagationResult.split(result.rightNodeUid, result.separatorKey);
        }
    }

    public void close() {
        bootRecord.close();
    }

    /**
     * 递归插入结束后，child split 向父 Node 传播的数据。
     * rightNodeUid 为 0 表示当前子树未发生需要向上传播的 split。
     */
    private static final class SplitPropagationResult {
        /** split 新建的右侧 Node UID；0 表示无需向上传播。 */
        final long rightNodeUid;
        /** 父 Node 连接左右 child 时写入的 separator key。 */
        final long separatorKey;

        private SplitPropagationResult(long rightNodeUid, long separatorKey) {
            this.rightNodeUid = rightNodeUid;
            this.separatorKey = separatorKey;
        }

        private static SplitPropagationResult noSplit() {
            return new SplitPropagationResult(0, 0);
        }

        private static SplitPropagationResult split(long rightNodeUid, long separatorKey) {
            return new SplitPropagationResult(rightNodeUid, separatorKey);
        }
    }
}
