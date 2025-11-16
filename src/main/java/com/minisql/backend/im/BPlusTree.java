package com.minisql.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.common.SubArray;
import com.minisql.backend.dm.DataManager;
import com.minisql.backend.dm.dataItem.DataItem;
import com.minisql.backend.im.Node.InsertAndSplitRes;
import com.minisql.backend.im.Node.LeafSearchRangeRes;
import com.minisql.backend.im.Node.SearchNextRes;
import com.minisql.backend.tm.TransactionManagerImpl;
import com.minisql.backend.utils.Parser;

public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;

    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
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
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 找到第一个大于等于 key 的节点 uid
     * @param nodeUid 任意一个非叶子节点
     * @param key 要查找的 key
     * @return 找到的节点 uid
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
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
            Node leaf = Node.loadNode(this, leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
