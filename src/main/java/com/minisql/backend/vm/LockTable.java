package com.minisql.backend.vm;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.common.Error;

/**
 * 维护等待图 + 死锁检测 + 等待/唤醒
 */
public class LockTable {

    /** xid -> 事务节点 */
    private final Map<Long, TNode> transactions = new HashMap<>();

    /** uid -> 资源节点 */
    private final Map<Long, UNode> resources = new HashMap<>();

    /** 全局保护锁，保护上面两个 Map 和图结构 */
    private final Lock graphLock = new ReentrantLock();

    /** DFS 用时间戳 */
    private int stamp = 1;

    /**
     * 申请锁：
     *  - 不需要等待：返回 null
     *  - 需要等待：返回 CountDownLatch，调用方在 latch.await() 处阻塞
     *  - 如果形成死锁：抛 DeadlockException
     */
    public CountDownLatch add(long xid, long uid) throws Exception {
        graphLock.lock();
        try {
            TNode tNode = getOrCreateTNode(xid);
            UNode uNode = getOrCreateUNode(uid);

            // 1. 已经持有该资源，直接返回
            if (tNode.isHolding(uNode)) {
                return null;
            }

            // 2. 资源没人持有，直接获得
            if (!uNode.isHeld()) {
                uNode.setHolder(tNode);
                tNode.addHolding(uNode);
                return null;
            }

            // 3. 资源被别人持有，建立等待关系
            setupWaiting(tNode, uNode);

            // 4. 死锁检测
            // 当前策略是谁造成死锁谁回滚
            if (hasDeadlock()) {
                // 回滚刚刚建立的等待关系
                cleanupWaiting(tNode, uNode);
                throw Error.DeadlockException;
            }

            // 5. 确认不会死锁，创建一个一次性 latch 返回给调用方
            if (tNode.getLatch() == null) {
                tNode.setLatch(new CountDownLatch(1));
            }
            return tNode.getLatch();

        } finally {
            graphLock.unlock();
        }
    }

    /**
     * 事务结束时释放它持有的所有资源，并唤醒等待者
     */
    public void remove(long xid) {
        graphLock.lock();
        try {
            TNode tNode = transactions.get(xid);
            if (tNode == null) return;

            // 1. 释放所有持有的资源，依次为每个资源选一个新的持有者
            for (UNode uNode : tNode.getHoldingSnapshot()) {
                uNode.clearHolder();
                assignNextHolder(uNode);
            }

            // 2. 如果它本身还在某个资源的等待队列里，清理掉
            if (tNode.isWaiting()) {
                UNode waitingFor = tNode.getWaiting();
                waitingFor.removeWaiter(tNode);
            }

            // 3. 从事务表中删除
            transactions.remove(xid);
            tNode.clearWaiting();
            // latch 留着也无所谓，反正这个 TNode 也被移除了

        } finally {
            graphLock.unlock();
        }
    }

    // ---------- 内部逻辑 ----------

    /** 建立等待关系：tNode 等待 uNode */
    private void setupWaiting(TNode tNode, UNode uNode) {
        tNode.setWaiting(uNode);
        uNode.addWaiter(tNode);
    }

    /** 清理等待关系（死锁检测失败时用） */
    private void cleanupWaiting(TNode tNode, UNode uNode) {
        tNode.clearWaiting();
        uNode.removeWaiter(tNode);
    }

    /**
     * 某个资源释放后，从等待队列中选一个新的事务作为持有者，并唤醒它
     */
    private void assignNextHolder(UNode uNode) {
        if (!uNode.hasWaiters()) return;

        Iterator<TNode> it = uNode.getWaiters().iterator();
        while (it.hasNext()) {
            TNode next = it.next();

            // 事务仍存在，并且还在等待这个资源
            if (transactions.containsKey(next.xid) &&
                    next.getWaiting() == uNode) {

                // 把资源交给它
                uNode.setHolder(next);
                next.addHolding(uNode);
                next.clearWaiting();

                // 唤醒等待它的线程
                CountDownLatch latch = next.getLatch();
                if (latch != null) {
                    // 这里可以在任意线程调用，不需要是“等待线程”
                    latch.countDown();
                }

                it.remove();
                break;
            } else {
                // 无效等待者，清理
                it.remove();
            }
        }

        // 如果等待队列空了，可以考虑移除资源节点（非必须）
        if (!uNode.hasWaiters() && !uNode.isHeld()) {
            resources.remove(uNode.uid);
        }
    }

    /** 死锁检测：有环返回 true */
    private boolean hasDeadlock() {
        // 1. 所有事务节点的 stamp 清零
        for (TNode t : transactions.values()) {
            t.setStamp(0);
        }

        // 2. 遍历所有事务节点，作为 DFS 的起点
        for (TNode t : transactions.values()) {
            if (t.getStamp() > 0) continue;

            stamp++;
            if (dfs(t)) {
                return true;
            }
        }
        return false;
    }

    /** DFS 沿着“等待边”走，看是否形成环 */
    private boolean dfs(TNode t) {
        if (t.getStamp() == stamp) {
            // 在同一轮 DFS 中再次遇到自己，说明有环
            return true;
        }
        if (t.getStamp() > 0 && t.getStamp() < stamp) {
            // 在旧的一轮 DFS 访问过，旧轮次路径已知无环，故不参与本次DFS
            return false;
        }

        t.setStamp(stamp);

        UNode uNode = t.getWaiting();
        if (uNode == null) return false;

        TNode holder = uNode.getHolder();
        if (holder == null) return false;

        return dfs(holder);
    }

    /** 获取或创建 TNode */
    private TNode getOrCreateTNode(long xid) {
        return transactions.computeIfAbsent(xid, k -> new TNode(xid));
    }

    /** 获取或创建 UNode */
    private UNode getOrCreateUNode(long uid) {
        return resources.computeIfAbsent(uid, k -> new UNode(uid));
    }

    // ---------- 内部节点类 ----------

    /** 事务节点 */
    private static class TNode {
        final long xid;
        private final List<UNode> holding = new ArrayList<>();
        private UNode waiting;
        private CountDownLatch latch; // 用于等待/唤醒
        private int stamp;

        TNode(long xid) {
            this.xid = xid;
        }

        void addHolding(UNode u) {
            holding.add(u);
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
