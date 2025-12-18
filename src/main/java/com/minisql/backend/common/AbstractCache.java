package com.minisql.backend.common;

import com.minisql.common.Error;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit; // 引入 TimeUnit
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 引用计数 + LRU 的并发安全缓存
 * 容量满时优先淘汰“最近最少使用且引用计数为零”的缓存项
 */
public abstract class AbstractCache<T> {
    
    // 字段名优化：使用 Resource<T>
    /** 维护缓存项的 LRU 容器，同时存储值和引用计数 */
    private final LinkedHashMap<Long, Resource<T>> cache; 
    /** 正在加载的 key 集合，避免重复加载 */
    private final Map<Long, Boolean> loading;
    /** 最大容量（0 表示不限制） */
    private final int capacity;
    private int size = 0;
    private final Lock lock = new ReentrantLock();

    private static final long LOAD_WAIT_MILLIS = 1;

    /**
     * 内部类：缓存中的资源项，封装了实际值和引用计数。
     */
    private static class Resource<T> {
        final T value;
        int referenceCount = 1;

        Resource(T value) {
            this.value = value;
        }
    }
    public AbstractCache(int capacity) {
        this.capacity = capacity;
        // LinkedHashMap 的 accessOrder=true 确保 LRU 特性
        this.cache = new LinkedHashMap<>(16, 0.75f, true);
        this.loading = new HashMap<>();
    }

    /**
     * 获取资源，命中则增加引用，未命中则加载并可能触发 LRU 淘汰
     * * @param key 缓存键
     * @return 实际资源对象
     * @throws Exception 如果加载失败或缓存已满且无法淘汰
     */
    protected T get(long key) throws Exception {
        
        // 1. 锁内处理：尝试命中、检查容量、标记加载
        lock.lock();
        try {
            // 优化：处理并发加载，等待其他线程完成加载
            while (loading.containsKey(key)) {
                lock.unlock(); // 释放锁，允许其他线程访问或释放缓存
                try {
                    TimeUnit.MILLISECONDS.sleep(LOAD_WAIT_MILLIS);
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt(); 
                }
                lock.lock(); // 重新获取锁，再次检查
            }
            // 缓存命中
            Resource<T> hit = cache.get(key);
            if (hit != null) {
                hit.referenceCount++; // 引用计数递增
                return hit.value;
            }
            // 未命中，检查容量
            if (capacity > 0 && size >= capacity) {
                if (!evictEldest()) {
                    throw Error.CacheFullException;
                }
            }
            // 标记正在加载
            loading.put(key, true);
        } finally {
            lock.unlock();
        }

        // 2. 锁外处理：加载资源（防止阻塞
        T obj;
        try {
            obj = loadCache(key); // 实际加载
        } catch (Exception e) {
            // 加载失败，回滚 loading 标记
            lock.lock();
            try {
                loading.remove(key);
            } finally {
                lock.unlock();
            }
            throw e;
        }

        // 3. 锁内处理：写入缓存
        lock.lock();
        try {
            loading.remove(key);
            
            // 优化：双重检查，处理并发加载竞争
            if (cache.containsKey(key)) {
                // 如果在锁外加载时，有其他线程抢先写入了，释放当前加载的 obj，使用已存在的
                safelyFlush(obj);
                Resource<T> existingItem = cache.get(key);
                existingItem.referenceCount++; 
                return existingItem.value; 
            }
            // 写入缓存，初始引用计数为 1
            cache.put(key, new Resource<>(obj));
            size++;
        } finally {
            lock.unlock();
        }
        return obj;
    }

    /**
     * 归还一次引用，引用计数减 1
     */
    protected void release(long key) {
        lock.lock();
        try {
            Resource<T> e = cache.get(key);
            if (e != null && e.referenceCount > 0) {
                e.referenceCount--;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            for (Resource<T> e : cache.values()) {
                safelyFlush(e.value);
            }
            cache.clear();
            loading.clear();
            size = 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * LRU 驱逐一个引用计数为 0 的条目，头部是旧的，尾部是新的
     */
    private boolean evictEldest() {
        // 使用 iterator 遍历 LinkedHashMap 的 LRU 顺序
        Iterator<Map.Entry<Long, Resource<T>>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Resource<T>> entry = it.next();
            Resource<T> e = entry.getValue();
            
            // 查找引用计数为 0 的项
            if (e.referenceCount == 0) {
                safelyFlush(e.value); // 写回资源
                it.remove(); // 淘汰
                size--;
                return true;
            }
        }
        return false;
    }

    private void safelyFlush(T obj) {
        try {
            flushCache(obj);
        } catch (Exception ignore) {
            // 忽略释放时的异常
        }
    }

    /**
     * 当资源不在缓存时的获取行为（由子类实现）
     */
    protected abstract T loadCache(long key) throws Exception;

    /**
     * 当资源被驱逐或关闭时的写回行为（由子类实现）
     */
    protected abstract void flushCache(T obj);
}
