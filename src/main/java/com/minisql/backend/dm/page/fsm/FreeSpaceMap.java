package com.minisql.backend.dm.page.fsm;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.dm.page.cache.PageCache;

/**
 * 记录每个页面剩余空间大小
 */
public class FreeSpaceMap {
    /**
     * FreeSpace 按剩余空间大小划分 40 个区间
     */
    private static final int LEVEL_COUNT = 40;
    /**
     * 每个区间大小为 1/40 页面大小
     */
    private static final int LEVEL_SIZE = PageCache.PAGE_SIZE / LEVEL_COUNT;

    private final Lock lock;

    // 每个 level 一个桶（队列），FIFO：先记录的先尝试
    private final Deque<FreeSpace>[] buckets;

    @SuppressWarnings("unchecked")
    public FreeSpaceMap() {
        lock = new ReentrantLock();
        buckets = new Deque[LEVEL_COUNT + 1];
        for (int i = 0; i <= LEVEL_COUNT; i++) {
            buckets[i] = new ArrayDeque<>();
        }
    }

    /**
     * 添加一个 FreeSpace
     * @param pgno 页号
     * @param freeSpaceSize 剩余空间大小
     */
    public void add(int pgno, int freeSpaceSize) {
        lock.lock();
        try {
            int level = freeSpaceSize / LEVEL_SIZE;
            if (level > LEVEL_COUNT) level = LEVEL_COUNT; // 防御：避免越界
            buckets[level].addLast(new FreeSpace(pgno, freeSpaceSize));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取一个满足 requiredSize 的 FreeSpace
     * @param requiredSize 需求空间大小
     * @return FreeSpace，找不到返回 null
     */
    public FreeSpace poll(int requiredSize) {
        lock.lock();
        try {
            int level = requiredSize / LEVEL_SIZE;
            if (level < LEVEL_COUNT) level++;
            while (level <= LEVEL_COUNT) {
                FreeSpace fs = buckets[level].pollFirst(); // O(1)，空则返回 null
                if (fs != null) {
                    return fs;
                }
                level++;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
