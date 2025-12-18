package com.minisql.backend.vm;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.common.Error;

/**
 * 维护等待图 + 死锁检测 + 等待/唤醒
 */
public class LockManager {

    /** xid -> 事务节点 */
    private final Map<Long, TNode> transactions = new HashMap<>();

    /** uid -> 资源节点 */
    private final Map<Long, UNode> resources = new HashMap<>();

    /** 全局保护锁，保护上面两个 Map 和图结构 */
    private final Lock graphLock = new ReentrantLock();

    /** DFS 用时间戳 */
    private int stamp = 1;

    public CountDownLatch acquire(long xid, long uid) throws Exception {
        graphLock.lock();
        try {
            TNode tNode = getOrCreateTNode(xid);
            UNode uNode = getOrCreateUNode(uid);

            // 1. 已经持有该资源
            if (tNode.isHolding(uNode)) {
                return null;
            }

            // 2. 资源无人持有，直接获得
            if (!uNode.isHeld()) {
                uNode.setHolder(tNode);
                tNode.addHolding(uNode);
                return null;
            }

            // 3. 资源被其他事务持有，则建立等待关系，每次等待都创建全新 latch
            setupWaiting(tNode, uNode);

            // 4. 死锁检测
            if (hasDeadlock()) {
                cleanupWaiting(tNode, uNode);
                throw Error.DeadlockException;
            }

            // 5. 返回本次等待的 latch
            return tNode.getLatch();

        } finally {
            graphLock.unlock();
        }
    }

    /**
     * 事务结束时释放它持有的所有资源，并唤醒等待者
     * 注意：按你的要求，这里不对“仍在等待”的事务做 countDown 解除阻塞
     */
    public void clear(long xid) {
        graphLock.lock();
        try {
            TNode tNode = transactions.get(xid);
            if (tNode == null) return;

            // 1. 释放所有持有的资源
            for (UNode uNode : tNode.getHoldingSnapshot()) {
                uNode.clearHolder();
                assignNextHolder(uNode);
            }

            // 2. 如果它本身还在某个资源的等待队列里，清理掉等待边并唤醒线程
            if (tNode.isWaiting()) {
                UNode waitingFor = tNode.getWaiting();
                if (waitingFor != null) {
                    waitingFor.removeWaiter(tNode);
                }

                // 唤醒可能阻塞在 await 上的线程
                CountDownLatch latch = tNode.getLatch();
                if (latch != null) {
                    latch.countDown();
                }
                tNode.clearWaiting();
                tNode.setLatch(null); // 清理引用，避免后续复用
            }

            // 3. 从事务表中删除
            transactions.remove(xid);

        } finally {
            graphLock.unlock();
        }
    }

    /**
     * 释放指定资源的锁，不结束事务，用于更新过程中切换到最新版本。
     */
    public void release(long xid, long uid) {
        graphLock.lock();
        try {
            TNode tNode = transactions.get(xid);
            UNode uNode = resources.get(uid);
            if (tNode == null || uNode == null) return;

            // 如果正在持有该资源，释放并唤醒下一个等待者
            if (tNode.isHolding(uNode)) {
                tNode.removeHolding(uNode);
                uNode.clearHolder();
                assignNextHolder(uNode);
            } else if (tNode.isWaiting() && tNode.getWaiting() == uNode) {
                // 如果正等待这个资源，清理等待边并唤醒自己
                uNode.removeWaiter(tNode);
                CountDownLatch latch = tNode.getLatch();
                if (latch != null) {
                    latch.countDown();
                }
                tNode.clearWaiting();
                tNode.setLatch(null);
            }

            if (!uNode.hasWaiters() && !uNode.isHeld()) {
                resources.remove(uid);
            }

        } finally {
            graphLock.unlock();
        }
    }

    // ---------- 内部逻辑 ----------

    private void setupWaiting(TNode tNode, UNode uNode) {
        tNode.setWaiting(uNode);
        tNode.setLatch(new CountDownLatch(1)); // 每次等待边都新建
        uNode.addWaiter(tNode);
    }

    private void cleanupWaiting(TNode tNode, UNode uNode) {
        tNode.clearWaiting();
        tNode.setLatch(null); // 死锁回滚时清掉本次等待 latch
        uNode.removeWaiter(tNode);
    }

    private void assignNextHolder(UNode uNode) {
        if (!uNode.hasWaiters()) return;

        Iterator<TNode> it = uNode.getWaiters().iterator();
        while (it.hasNext()) {
            TNode next = it.next();

            if (transactions.containsKey(next.xid) && next.getWaiting() == uNode) {

                // 交付资源
                uNode.setHolder(next);
                next.addHolding(uNode);
                next.clearWaiting();

                // 唤醒（这是“授予锁”的唤醒）
                CountDownLatch latch = next.getLatch();
                if (latch != null) {
                    latch.countDown();
                }
                next.setLatch(null); // 唤醒后清理，避免复用

                it.remove();
                break;
            } else {
                it.remove();
            }
        }

        if (!uNode.hasWaiters() && !uNode.isHeld()) {
            resources.remove(uNode.uid);
        }
    }

    private boolean hasDeadlock() {
        for (TNode t : transactions.values()) {
            t.setStamp(0);
        }

        for (TNode t : transactions.values()) {
            if (t.getStamp() > 0) continue;
            stamp++;
            if (dfs(t)) return true;
        }
        return false;
    }

    private boolean dfs(TNode t) {
        if (t.getStamp() == stamp) return true;
        if (t.getStamp() > 0 && t.getStamp() < stamp) return false;

        t.setStamp(stamp);

        UNode uNode = t.getWaiting();
        if (uNode == null) return false;

        TNode holder = uNode.getHolder();
        if (holder == null) return false;

        return dfs(holder);
    }

    private TNode getOrCreateTNode(long xid) {
        return transactions.computeIfAbsent(xid, k -> new TNode(xid));
    }

    private UNode getOrCreateUNode(long uid) {
        return resources.computeIfAbsent(uid, k -> new UNode(uid));
    }

    // ---------- 内部节点类 ----------

    /** 事务节点 */
    private static class TNode {
        final long xid;
        private final List<UNode> holding = new ArrayList<>();
        private UNode waiting;
        private CountDownLatch latch; // 用于等待/唤醒（只代表“本次等待边”）
        private int stamp;

        TNode(long xid) {
            this.xid = xid;
        }

        void addHolding(UNode u) {
            holding.add(u);
        }

        void removeHolding(UNode u) {
            holding.remove(u);
        }

        List<UNode> getHoldingSnapshot() {
            return new ArrayList<>(holding);
        }

        void setWaiting(UNode u) {
            this.waiting = u;
        }

        UNode getWaiting() {
            return waiting;
        }

        boolean isHolding(UNode u) {
            return holding.contains(u);
        }

        boolean isWaiting() {
            return waiting != null;
        }

        void clearWaiting() {
            this.waiting = null;
        }

        CountDownLatch getLatch() {
            return latch;
        }

        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        int getStamp() {
            return stamp;
        }

        void setStamp(int stamp) {
            this.stamp = stamp;
        }
    }

    /** 资源节点 */
    private static class UNode {
        final long uid;
        private TNode holder;
        // FIFO 列表
        private final List<TNode> waiters = new ArrayList<>();

        UNode(long uid) {
            this.uid = uid;
        }

        boolean isHeld() {
            return holder != null;
        }

        void setHolder(TNode t) {
            this.holder = t;
        }

        void clearHolder() {
            this.holder = null;
        }

        TNode getHolder() {
            return holder;
        }

        void addWaiter(TNode t) {
            waiters.add(t);
        }

        void removeWaiter(TNode t) {
            waiters.remove(t);
        }

        List<TNode> getWaiters() {
            return waiters;
        }

        boolean hasWaiters() {
            return !waiters.isEmpty();
        }
    }
}
