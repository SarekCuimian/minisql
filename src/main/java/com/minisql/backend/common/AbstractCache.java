package com.minisql.backend.common;

import com.minisql.common.Error;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 并发缓存（引用计数 + LRU）
 * <p>
 * LinkedHashMap 仅维护 LRU 顺序；额外 index(CHM) 用于 O(1) 定位 Resource，
 * 从而 release() 不触发 accessOrder 的顺序变化。
 * </p>
 * 关键目标：
 * 1) 不同 key 的 get 并发
 * 2) 相同 key 的 miss 只允许一个线程加载（SingleFlight），其余线程等待同一个结果
 * 3) 淘汰：容量满时优先淘汰“LRU 且引用计数为 0”的条目
 *
 * 重要修正：
 * - 不再用 waiters 由 loader 一次性补 refCount
 * - 改为每个 get() 调用者在成功返回前自行 ref++，与 release() ref-- 严格对称
 */
public abstract class AbstractCache<T> {

    /** LRU 容器：accessOrder=true，命中会调整顺序 => 需要结构锁保护 */
    private final LinkedHashMap<Long, Resource<T>> cache;

    /** O(1) 定位：release 不走 cache.get，从而不影响 LRU 顺序 */
    private final ConcurrentHashMap<Long, Resource<T>> index = new ConcurrentHashMap<>();

    /** 正在加载/等待加载的 key（SingleFlight 状态表） */
    private final ConcurrentHashMap<Long, LoadState<T>> pendingLoads = new ConcurrentHashMap<>();

    /** 最大容量（0 表示不限制） */
    private final int capacity;

    private int size = 0;

    /** 保护 LinkedHashMap 结构与 size、淘汰等 */
    private final ReentrantLock lruLock = new ReentrantLock();

    /** 分段 key 锁：用于同 key 的“装载/写入缓存/容量淘汰”过程串行化 */
    private final ReentrantLock[] keyLocks;

    private ReentrantLock getKeyLock(long key) {
        long h = key ^ (key >>> 32);
        h ^= (h >>> 16);
        int idx = ((int) h) & (keyLocks.length - 1);
        return keyLocks[idx];
    }

    /** 缓存项：值 + 引用计数（Atomic，便于 release 无锁 ref--） */
    private static class Resource<T> {
        final T value;
        final AtomicInteger refCount = new AtomicInteger(0);

        Resource(T value) {
            this.value = value;
        }
    }

    /**
     * 单飞加载状态：
     * - future：所有等待者共享同一个结果
     * - started：保证只有一个线程真正执行 load
     */
    private static class LoadState<T> {
        final CompletableFuture<T> future = new CompletableFuture<>();
        final AtomicBoolean started = new AtomicBoolean(false);
    }

    public AbstractCache(int capacity) {
        this(capacity, 64);
    }

    /**
     * @param stripes 分段锁数量，建议 2 的幂（32/64/128...）
     */
    public AbstractCache(int capacity, int stripes) {
        this.capacity = capacity;
        this.cache = new LinkedHashMap<>(16, 0.75f, true);
        int len = Math.max(4, stripes);
        this.keyLocks = new ReentrantLock[len];
        for (int i = 0; i < keyLocks.length; i++) {
            keyLocks[i] = new ReentrantLock();
        }
    }

    /**
     * 获取资源：
     * - 命中：ref++，并触发 LRU touch（LinkedHashMap.get）
     * - miss：SingleFlight（同 key 仅一个线程 load，其余等待同一个 future）
     *
     * 重要：miss 情况下，不再由 loader 批量补 refCount；
     *      改为每个调用者在成功返回前自行 ref++，与 release 对称。
     */
    protected T get(long key) throws Exception {
        // 1) 快速命中（命中会移动 LRU 顺序 => 必须 lruLock）
        lruLock.lock();
        try {
            Resource<T> hit = cache.get(key);
            if (hit != null) {
                hit.refCount.incrementAndGet();
                return hit.value;
            }
        } finally {
            lruLock.unlock();
        }

        // 2) miss：进入 pendingLoads（SingleFlight）
        LoadState<T> state = pendingLoads.computeIfAbsent(key, k -> new LoadState<>());

        // 3) loader：同 key 仅一个线程真正 load
        if (state.started.compareAndSet(false, true)) {
            ReentrantLock keyLock = getKeyLock(key);
            keyLock.lock();
            try {
                // 3.1 double-check：成为 loader 后再确认缓存是否已有
                lruLock.lock();
                try {
                    Resource<T> exist = cache.get(key); // 这里会 touch：表示确实在处理一次 get 访问
                    if (exist != null) {
                        // 只给 loader 自己加一次引用；其他等待者会在 future 返回后各自 ref++
                        exist.refCount.incrementAndGet();
                        state.future.complete(exist.value);
                        return exist.value;
                    }
                } finally {
                    lruLock.unlock();
                }

                // 3.2 真正加载（锁外 IO/计算）
                T v = null;
                T evicted = null;
                try {
                    v = loadCache(key);

                    // 3.3 写入缓存 + 容量淘汰（持有 lruLock）
                    Resource<T> r = new Resource<>(v);
                    lruLock.lock();
                    try {
                        if (capacity > 0 && size >= capacity) {
                            evicted = evictOne();
                            if (evicted == null) {
                                // 无法淘汰：说明所有条目都 ref>0
                                safelyFlush(v);
                                state.future.completeExceptionally(Error.CacheFullException);
                                throw Error.CacheFullException;
                            }
                        }
                        cache.put(key, r);
                        index.put(key, r);
                        size++;
                        // loader 自己先持有一次引用，避免刚插入就被别的线程淘汰（ref==0 才能淘汰）
                        r.refCount.incrementAndGet();
                    } finally {
                        lruLock.unlock();
                    }
                    if (evicted != null) safelyFlush(evicted);
                    state.future.complete(v);
                    return v;

                } catch (Exception e) {
                    if (evicted != null) safelyFlush(evicted);
                    if (v != null) safelyFlush(v);
                    state.future.completeExceptionally(e);
                    throw e;
                }

            } finally {
                keyLock.unlock();
                pendingLoads.remove(key, state);
            }
        }

        // 4) 非 loader：等待同一个 future，然后“自己 ref++（并 touch LRU）”
        T v;
        try {
            v = state.future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (ExecutionException ee) {
            Throwable c = ee.getCause();
            if (c instanceof Exception) throw (Exception) c;
            throw new RuntimeException(c);
        }

        // future 成功意味着 loader 已完成写入缓存（或至少应如此）
        // 这里用 cache.get(key) 让这次 get 也计入 LRU 访问顺序
        lruLock.lock();
        try {
            Resource<T> r = cache.get(key); // touch LRU
            if (r == null) {
                // 理论上不应发生（除非 close/清空与等待竞争），直接抛出让调用方感知异常
                throw new IllegalStateException("Cache entry missing after load completion (key=" + key + ")");
            }
            r.refCount.incrementAndGet();
        } finally {
            lruLock.unlock();
        }

        return v;
    }

    /**
     * 归还一次引用（ref--），不影响 LRU 顺序：
     * - 不调用 cache.get（避免 accessOrder 变化）
     * - 通过 index O(1) 定位 Resource
     */
    protected void release(long key) {
        Resource<T> r = index.get(key);
        if (r == null) return;

        // 避免减成负数（并发下 close/evict 也可能发生）
        while (true) {
            int cur = r.refCount.get();
            if (cur <= 0) return;
            if (r.refCount.compareAndSet(cur, cur - 1)) return;
        }
    }

    /**
     * 关闭缓存：写回并清空
     * 同时让所有 pending 的 future 结束，避免等待线程卡死
     */
    protected void close() {
        // 先让 pending 的等待者全部结束
        for (LoadState<T> state : pendingLoads.values()) {
            state.future.completeExceptionally(new IllegalStateException("Cache closed"));
        }

        lruLock.lock();
        try {
            for (Map.Entry<Long, Resource<T>> en : cache.entrySet()) {
                safelyFlush(en.getValue().value);
            }
            cache.clear();
            index.clear();
            pendingLoads.clear();
            size = 0;
        } finally {
            lruLock.unlock();
        }
    }

    /**
     * LRU 淘汰：淘汰最旧且 ref=0 的条目，返回被驱逐的对象供锁外 flush。
     * 注意：必须在持有 lruLock 的情况下调用
     */
    private T evictOne() {
        Iterator<Map.Entry<Long, Resource<T>>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Resource<T>> entry = it.next();
            long key = entry.getKey();
            Resource<T> r = entry.getValue();

            if (r.refCount.get() == 0) {
                // 先从 LRU 结构移除
                it.remove();
                size--;
                // 再从 index 移除（带 value 的 remove，避免删错新值）
                index.remove(key, r);
                return r.value;
            }
        }
        return null;
    }

    private void safelyFlush(T obj) {
        try {
            flushCache(obj);
        } catch (Exception ignore) {
            // 忽略释放时异常；如需可在此打日志/计数
        }
    }

    /** 资源不在缓存时的加载行为（子类实现） */
    protected abstract T loadCache(long key) throws Exception;

    /** 资源被驱逐或关闭时的写回行为（子类实现） */
    protected abstract void flushCache(T obj);
}
