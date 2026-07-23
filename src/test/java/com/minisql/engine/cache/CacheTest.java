package com.minisql.engine.cache;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.minisql.error.Error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicReference;

public class CacheTest {

    static Random random = new SecureRandom();

    private CountDownLatch cdl;
    private MockCache cache;
    private final AtomicInteger success = new AtomicInteger();
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicReference<Throwable> unexpectedFailure = new AtomicReference<>();
    private long startTs;

    @Test
    public void testCacheWithVariousCapacity() {
        // 可以调整这些参数做对比
        int[] capacities = new int[]{0, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 50, 20, 10, 5, 1}; // 0 表示无限制

        for (int cap : capacities) {
            runOnce(cap);
        }
    }

    private void runOnce(int capacity) {
        cache = new MockCache(capacity);
        cdl = new CountDownLatch(100);
        success.set(0);
        errors.set(0);
        unexpectedFailure.set(null);
        startTs = System.currentTimeMillis();
        for(int i = 0; i < 100; i ++) {
            Runnable r = this::work;
            new Thread(r, "cache-worker-" + i).start();
        }
        try {
            // 主线程阻塞，等待所有子线程执行完毕
            cdl.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("interrupted while waiting for cache workers", e);
        }
        long elapsed = System.currentTimeMillis() - startTs;
        System.out.println("CacheTest(capacity=" + capacity + ") done, success=" + success.get() + ", errors=" + errors.get()
                + ", elapsed(ms)=" + elapsed);
        if (unexpectedFailure.get() != null) {
            fail("cache worker failed", unexpectedFailure.get());
        }
        assertEquals(1_000_000, success.get() + errors.get(), "every cache operation must be accounted for");
    }

    private void work() {
        try {
            for(int i = 0; i < 10000; i++) {
                long uid = random.nextInt();
                long h;
                try {
                    h = cache.get(uid);
                } catch (Exception e) {
                    if(e == Error.CacheFullException) {
                        errors.incrementAndGet();
                        continue;
                    }
                    unexpectedFailure.compareAndSet(null, e);
                    return;
                }
                assertEquals(uid, h);
                cache.release(h);
                success.incrementAndGet();
            }
        } catch (Throwable e) {
            unexpectedFailure.compareAndSet(null, e);
        } finally {
            cdl.countDown();
        }
    }
}
