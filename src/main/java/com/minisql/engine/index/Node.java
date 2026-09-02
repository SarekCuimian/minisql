package com.minisql.engine.index;

import java.util.ArrayList;
import java.util.List;

import com.minisql.engine.storage.codec.ByteSlice;
import com.minisql.engine.storage.codec.ByteUtil;
import com.minisql.engine.storage.record.PageRecord;
import com.minisql.engine.transaction.xid.XidAllocator;

/**
 * B+Tree Node 的二进制布局：
 * [LeafFlag][KeyCount][SiblingUid]
 * [ChildUid0][Key0][ChildUid1][Key1]...[ChildUidN][KeyN]
 */
public class Node implements AutoCloseable {
    static final int IS_LEAF_OFFSET = 0;
    static final int KEY_COUNT_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_UID_OFFSET = KEY_COUNT_OFFSET + Short.BYTES;
    static final int NODE_HEADER_SIZE = SIBLING_UID_OFFSET + Long.BYTES;
    static final int ENTRY_SIZE = Long.BYTES * 2;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + ENTRY_SIZE * (BALANCE_NUMBER * 2 + 2);

    private final BPlusTree tree;
    private final PageRecord record;
    private final ByteSlice nodeBytes;

    private Node(BPlusTree tree, PageRecord record) {
        this.tree = tree;
        this.record = record;
        this.nodeBytes = record.payload();
    }

    /**
     * 根据 split 后的左右 Node 创建新的 root bytes。
     *
     * @param leftUid 左侧 child UID
     * @param rightUid 右侧 child UID
     * @param separatorKey 划分左右 child key range 的 separator key
     */
    static byte[] newRootBytes(long leftUid, long rightUid, long separatorKey) {
        ByteSlice rootBytes = new ByteSlice(new byte[NODE_SIZE], 0, NODE_SIZE);

        setLeaf(rootBytes, false);
        setKeyCount(rootBytes, 2);
        setSiblingUid(rootBytes, 0);
        setChildUid(rootBytes, 0, leftUid);
        setKey(rootBytes, 0, separatorKey);
        setChildUid(rootBytes, 1, rightUid);
        setKey(rootBytes, 1, Long.MAX_VALUE);

        return rootBytes.bytes();
    }

    /** 创建空 B+Tree 的初始 root bytes。 */
    static byte[] newEmptyRootBytes() {
        ByteSlice rootBytes = new ByteSlice(new byte[NODE_SIZE], 0, NODE_SIZE);

        setLeaf(rootBytes, true);
        setKeyCount(rootBytes, 0);
        setSiblingUid(rootBytes, 0);

        return rootBytes.bytes();
    }

    static Node load(BPlusTree tree, long uid) throws Exception {
        PageRecord record = tree.pageRecordManager.acquire(uid);
        assert record != null;
        return new Node(tree, record);
    }

    /** 归还加载该 Node 时持有的 Page 引用。 */
    @Override
    public void close() {
        record.close();
    }

    public boolean isLeaf() {
        record.rLock();
        try {
            return isLeaf(nodeBytes);
        } finally {
            record.rUnlock();
        }
    }

    /**
     * 根据 separator keys 定位目标 child；若 key 超出当前 Node 的范围，则返回右侧 sibling。
     */
    ChildLookupResult lookupChild(long key) {
        record.rLock();
        try {
            int keyCount = getKeyCount(nodeBytes);
            for (int i = 0; i < keyCount; i++) {
                long currentKey = getKey(nodeBytes, i);
                if (key < currentKey) {
                    return new ChildLookupResult(getChildUid(nodeBytes, i), 0);
                }
            }
            return new ChildLookupResult(0, getSiblingUid(nodeBytes));
        } finally {
            record.rUnlock();
        }
    }

    /** 查找当前 leaf Node 中位于闭区间 [leftKey, rightKey] 的 entries。 */
    RangeSearchResult searchRange(long leftKey, long rightKey) {
        record.rLock();
        try {
            int keyCount = getKeyCount(nodeBytes);
            int index = 0;
            while (index < keyCount) {
                long currentKey = getKey(nodeBytes, index);
                if (currentKey >= leftKey) {
                    break;
                }
                index++;
            }

            List<Long> uids = new ArrayList<>();
            while (index < keyCount) {
                long currentKey = getKey(nodeBytes, index);
                if (currentKey > rightKey) {
                    break;
                }
                uids.add(getChildUid(nodeBytes, index));
                index++;
            }

            long siblingUid = index == keyCount ? getSiblingUid(nodeBytes) : 0;
            return new RangeSearchResult(uids, siblingUid);
        } finally {
            record.rUnlock();
        }
    }

    InsertResult insertAndSplit(long uid, long key) throws Exception {
        boolean inserted = false;
        Exception error = null;

        record.startUpdate();
        try {
            inserted = insert(uid, key);
            if (!inserted) {
                return InsertResult.retryAt(getSiblingUid(nodeBytes));
            }
            if (!needsSplit()) {
                return InsertResult.inserted();
            }

            try {
                SplitResult splitResult = split();
                return InsertResult.split(
                        splitResult.rightNodeUid,
                        splitResult.separatorKey
                );
            } catch (Exception exception) {
                error = exception;
                throw exception;
            }
        } finally {
            if (error == null && inserted) {
                record.finishUpdate(XidAllocator.SYSTEM_XID);
            } else {
                record.abortUpdate();
            }
        }
    }

    private boolean insert(long uid, long key) {
        int keyCount = getKeyCount(nodeBytes);
        int index = 0;
        while (index < keyCount) {
            long currentKey = getKey(nodeBytes, index);
            if (currentKey < key) {
                index++;
            } else {
                break;
            }
        }
        if (index == keyCount && getSiblingUid(nodeBytes) != 0) {
            return false;
        }

        if (isLeaf(nodeBytes)) {
            shiftEntriesRightFrom(nodeBytes, index);
            setKey(nodeBytes, index, key);
            setChildUid(nodeBytes, index, uid);
            setKeyCount(nodeBytes, keyCount + 1);
        } else {
            long previousKey = getKey(nodeBytes, index);
            setKey(nodeBytes, index, key);
            shiftEntriesRightFrom(nodeBytes, index + 1);
            setKey(nodeBytes, index + 1, previousKey);
            setChildUid(nodeBytes, index + 1, uid);
            setKeyCount(nodeBytes, keyCount + 1);
        }
        return true;
    }

    private boolean needsSplit() {
        return BALANCE_NUMBER * 2 == getKeyCount(nodeBytes);
    }

    private SplitResult split() throws Exception {
        ByteSlice newNodeBytes = new ByteSlice(new byte[NODE_SIZE], 0, NODE_SIZE);
        setLeaf(newNodeBytes, isLeaf(nodeBytes));
        setKeyCount(newNodeBytes, BALANCE_NUMBER);
        setSiblingUid(newNodeBytes, getSiblingUid(nodeBytes));
        copyEntriesFrom(nodeBytes, BALANCE_NUMBER, newNodeBytes, BALANCE_NUMBER);

        long rightNodeUid = tree.pageRecordManager.insert(
                XidAllocator.SYSTEM_XID,
                newNodeBytes.bytes()
        );
        setKeyCount(nodeBytes, BALANCE_NUMBER);
        setSiblingUid(nodeBytes, rightNodeUid);

        return new SplitResult(rightNodeUid, getKey(newNodeBytes, 0));
    }

    /** 设置 Node 是否为 leaf。 */
    private static void setLeaf(ByteSlice nodeBytes, boolean leaf) {
        nodeBytes.bytes()[nodeBytes.offset() + IS_LEAF_OFFSET] = leaf ? (byte) 1 : (byte) 0;
    }

    /** 判断 Node 是否为 leaf。 */
    private static boolean isLeaf(ByteSlice nodeBytes) {
        return nodeBytes.bytes()[nodeBytes.offset() + IS_LEAF_OFFSET] == (byte) 1;
    }

    /** 设置 Node 中有效 entry 的数量。 */
    private static void setKeyCount(ByteSlice nodeBytes, int keyCount) {
        ByteUtil.putShort(nodeBytes.bytes(), nodeBytes.offset() + KEY_COUNT_OFFSET, (short) keyCount);
    }

    /** 获取 Node 中有效 entry 的数量。 */
    private static int getKeyCount(ByteSlice nodeBytes) {
        return ByteUtil.getShort(nodeBytes.bytes(), nodeBytes.offset() + KEY_COUNT_OFFSET);
    }

    /** 设置同层右侧 sibling Node 的 UID。 */
    private static void setSiblingUid(ByteSlice nodeBytes, long siblingUid) {
        ByteUtil.putLong(nodeBytes.bytes(), nodeBytes.offset() + SIBLING_UID_OFFSET, siblingUid);
    }

    /** 获取同层右侧 sibling Node 的 UID。 */
    private static long getSiblingUid(ByteSlice nodeBytes) {
        return ByteUtil.getLong(nodeBytes.bytes(), nodeBytes.offset() + SIBLING_UID_OFFSET);
    }

    /** 设置指定 entry 的 child UID。 */
    private static void setChildUid(ByteSlice nodeBytes, int index, long childUid) {
        ByteUtil.putLong(nodeBytes.bytes(), entryOffset(nodeBytes, index), childUid);
    }

    /** 获取指定 entry 的 child UID。 */
    private static long getChildUid(ByteSlice nodeBytes, int index) {
        return ByteUtil.getLong(nodeBytes.bytes(), entryOffset(nodeBytes, index));
    }

    /** 设置指定 entry 的 key。 */
    private static void setKey(ByteSlice nodeBytes, int index, long key) {
        ByteUtil.putLong(nodeBytes.bytes(), entryOffset(nodeBytes, index) + Long.BYTES, key);
    }

    /** 获取指定 entry 的 key。 */
    private static long getKey(ByteSlice nodeBytes, int index) {
        return ByteUtil.getLong(nodeBytes.bytes(), entryOffset(nodeBytes, index) + Long.BYTES);
    }

    /**
     * 从 sourceBytes 的 startIndex 开始复制 count 个 entries，
     * 写入 targetBytes 的第 0 个 entry。
     */
    private static void copyEntriesFrom(ByteSlice sourceBytes, int startIndex, ByteSlice targetBytes, int count) {
        System.arraycopy(
                sourceBytes.bytes(),
                entryOffset(sourceBytes, startIndex),
                targetBytes.bytes(),
                entryOffset(targetBytes, 0),
                count * ENTRY_SIZE
        );
    }

    /** 从 startIndex 开始，将有效 entries 整体右移一个位置。 */
    private static void shiftEntriesRightFrom(ByteSlice nodeBytes, int startIndex) {
        int keyCount = getKeyCount(nodeBytes);
        int sourceOffset = entryOffset(nodeBytes, startIndex);
        int targetOffset = entryOffset(nodeBytes, startIndex + 1);
        int byteCount = (keyCount - startIndex) * ENTRY_SIZE;
        System.arraycopy(nodeBytes.bytes(), sourceOffset, nodeBytes.bytes(), targetOffset, byteCount);
    }

    private static int entryOffset(ByteSlice nodeBytes, int index) {
        return nodeBytes.offset() + NODE_HEADER_SIZE + index * ENTRY_SIZE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Is leaf: ").append(isLeaf(nodeBytes)).append("\n");
        int keyCount = getKeyCount(nodeBytes);
        builder.append("Key count: ").append(keyCount).append("\n");
        builder.append("Sibling UID: ").append(getSiblingUid(nodeBytes)).append("\n");
        for (int i = 0; i < keyCount; i++) {
            builder.append("Child UID: ").append(getChildUid(nodeBytes, i))
                    .append(", key: ").append(getKey(nodeBytes, i)).append("\n");
        }
        return builder.toString();
    }

    /**
     * 在内部 Node 中查找下一级 child 的结果。
     * childUid 非 0 表示已在当前 Node 找到路由；否则应沿 siblingUid 继续查找。
     */
    static final class ChildLookupResult {
        /** 当前 Node 中匹配到的 child UID；0 表示需要继续访问 sibling。 */
        final long childUid;
        /** 当前 Node 右侧 sibling 的 UID；仅 childUid 为 0 时有效。 */
        final long siblingUid;

        private ChildLookupResult(long childUid, long siblingUid) {
            this.childUid = childUid;
            this.siblingUid = siblingUid;
        }
    }

    /**
     * 一次 leaf Node 范围查找的结果。
     * uids 为当前 Node 内命中的记录 UID；siblingUid 非 0 表示范围查找需继续访问右侧 leaf。
     */
    static final class RangeSearchResult {
        /** 当前 leaf Node 中命中的记录 UID。 */
        final List<Long> uids;
        /** 仍可能存在范围内记录时，需要继续访问的右侧 leaf UID。 */
        final long siblingUid;

        private RangeSearchResult(List<Long> uids, long siblingUid) {
            this.uids = uids;
            this.siblingUid = siblingUid;
        }
    }

    /**
     * 一次 Node 插入尝试的结果。
     * 三种状态互斥：直接插入、转向 sibling 重试，或 split 并返回右侧 Node 与 separator key。
     */
    static final class InsertResult {
        /** 重试时的右侧 sibling UID；0 表示当前 Node 已处理插入。 */
        final long siblingUid;
        /** split 后新建的右侧 Node UID；0 表示未发生 split。 */
        final long rightNodeUid;
        /** split 后向父 Node 传播的 separator key；仅 rightNodeUid 非 0 时有效。 */
        final long separatorKey;

        private InsertResult(long siblingUid, long rightNodeUid, long separatorKey) {
            this.siblingUid = siblingUid;
            this.rightNodeUid = rightNodeUid;
            this.separatorKey = separatorKey;
        }

        private static InsertResult inserted() {
            return new InsertResult(0, 0, 0);
        }

        private static InsertResult retryAt(long siblingUid) {
            return new InsertResult(siblingUid, 0, 0);
        }

        private static InsertResult split(long rightNodeUid, long separatorKey) {
            return new InsertResult(0, rightNodeUid, separatorKey);
        }
    }

    /**
     * split() 内部产生的临时结果。
     * 它只描述当前 Node 的拆分，不承担 sibling 重试或向上递归传播的职责。
     */
    private static final class SplitResult {
        /** split 新建的右侧 Node UID。 */
        final long rightNodeUid;
        /** 右侧 Node 的首个 key，也是向父 Node 写入的 separator key。 */
        final long separatorKey;

        private SplitResult(long rightNodeUid, long separatorKey) {
            this.rightNodeUid = rightNodeUid;
            this.separatorKey = separatorKey;
        }
    }
}
