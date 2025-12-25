package com.minisql.backend.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AbstractCache 的并发正确性 + 简易性能基准测试（JUnit5）
 *
 * 注意：
 * - 这是“单测形式的 micro-bench”，用于对比版本差异/观察瓶颈。
 * - 若要严格 benchmark，请用 JMH。
 */
public class AbstractCachePerfTest {

    /**
     * 一个可控的 Cache 子类：
     * - loadCache：可配置 sleep（模拟 IO）
     * - flushCache：默认 no-op
     * - 统计 load 次数
     */
    static class TestCache extends AbstractCache<byte[]> {
        private final int valueSizeBytes;
        private final int loadMillis;
        private final AtomicLong loadCount = new AtomicLong();

        TestCache(int capacity, int stripes, int valueSizeBytes, int loadMillis) {
            super(capacity, stripes);
            this.valueSizeBytes = valueSizeBytes;
            this.loadMillis = loadMillis;
        }

        @Override
        protected byte[] loadCache(long key) throws Exception {
            loadCount.incrementAndGet();
            if (loadMillis > 0) {
                Thread.sleep(loadMillis);
            }
            return new byte[valueSizeBytes];
        }

        @Override
        protected void flushCache(byte[] obj) {
            // no-op（如需模拟 flush 成本，可在这里 sleep 或计数）
        }

        long loads() {
            return loadCount.get();
        }
    }

    // -------------------------------
    // 工具：并发跑若干秒，打印 QPS/loads
    // -------------------------------
    private static void runBenchmark(
            String name,
            TestCache cache,
            int threads,
            int seconds,
            KeySupplier keySupplier,
            boolean doRelease
    ) throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicLong ops = new AtomicLong();

        long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    while (System.nanoTime() < endAt) {
                        long k = keySupplier.nextKey();
                        byte[] v = cache.get(k);
                        // 模拟“用一下”
                        if (v == null) throw new AssertionError("value is null");
                        if (doRelease) cache.release(k);
                        ops.incrementAndGet();
                    }
                } catch (Exception e) {
                    // 单测中直接抛会中断线程；这里打印出来方便定位
                    e.printStackTrace();
                } finally {
                    done.countDown();
                }
            });
        }

        // 预热一点点（可选）
        start.countDown();
        done.await();
        pool.shutdownNow();
        pool.awaitTermination(3, TimeUnit.SECONDS);

        double qps = ops.get() / (double) seconds;
        System.out.printf("%s | threads=%d, seconds=%d, ops=%d, qps=%.0f, loads=%d%n",
                name, threads, seconds, ops.get(), qps, cache.loads());
    }

    @FunctionalInterface
    interface KeySupplier {
        long nextKey();
    }

    // -------------------------------
    // 1) 正确性：SingleFlight 同 key 并发 miss 应只 load 1 次（或极少）
    // -------------------------------
    @Test
    @Timeout(10)
    void singleFlight_sameKey_onlyOneLoad() throws Exception {
        int threads = 32;
        long key = 42L;

        // load 模拟慢一些，放大并发窗口
        TestCache cache = new TestCache(/*capacity*/128, /*stripes*/64, /*value*/1024, /*loadMillis*/50);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    byte[] v = cache.get(key);
                    assertNotNull(v);
                    // 立刻 release，避免 ref 一直不归零
                    cache.release(key);
                } catch (Exception e) {
                    fail(e);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdownNow();

        // 理想情况下 loads=1；考虑极端调度/实现细节，允许很小波动
        long loads = cache.loads();
        assertTrue(loads <= 2, "expected loads<=2, but got " + loads);
    }

    // -------------------------------
    // 2) 性能：纯命中（预热后 100% hit），观察 lruLock 竞争的吞吐
    // -------------------------------
    @Test
    @Timeout(20)
    void perf_allHit_qps() throws Exception {
        int threads = 8;
        int seconds = 3;
        int keySpace = 10_000;

        TestCache cache = new TestCache(/*capacity*/keySpace + 10, /*stripes*/64, /*value*/256, /*loadMillis*/0);

        // 预热：把 keySpace 都塞进 cache
        for (int i = 0; i < keySpace; i++) {
            cache.get(i);
            cache.release(i);
        }

        runBenchmark(
                "[HIT] all-hit",
                cache,
                threads,
                seconds,
                () -> ThreadLocalRandom.current().nextInt(keySpace),
                true
        );

        // 纯 hit 下 loads 不应继续显著增长（可能有极少 miss，正常情况下接近 0）
        // 这里不做强断言，因为你可能改了容量/淘汰策略
    }

    // -------------------------------
    // 3) 性能：热点单 key（同 key 高并发）+ 慢 load，观察是否出现 loads 暴涨
    // -------------------------------
    @Test
    @Timeout(20)
    void perf_hotKey_singleFlight_qps_and_loads() throws Exception {
        int threads = 32;
        int seconds = 3;
        long hotKey = 1L;

        TestCache cache = new TestCache(/*capacity*/128, /*stripes*/64, /*value*/1024, /*loadMillis*/20);

        runBenchmark(
                "[HOT] single key",
                cache,
                threads,
                seconds,
                () -> hotKey,
                true
        );

        // 只要 singleflight 生效，loads 不应接近 ops（否则说明重复加载严重）
        // 因为 loadMillis=20ms，理论上几秒内 load 次数应很有限
        assertTrue(cache.loads() < 20, "loads too high for hotKey scenario: " + cache.loads());
    }

    // -------------------------------
    // 4) 性能：小容量 + 大 keySpace churn（频繁淘汰/加载），观察吞吐和 loads
    // -------------------------------
    @Test
    @Timeout(25)
    void perf_smallCapacity_churn_qps() throws Exception {
        int threads = 8;
        int seconds = 3;
        int keySpace = 50_000;

        // 容量很小：会频繁淘汰
        TestCache cache = new TestCache(/*capacity*/128, /*stripes*/64, /*value*/256, /*loadMillis*/0);

        runBenchmark(
                "[CHURN] small capacity, large keySpace",
                cache,
                threads,
                seconds,
                () -> ThreadLocalRandom.current().nextInt(keySpace),
                true
        );

        // 这里也不做强断言：churn 本身 loads 会很高是正常的
    }

    // -------------------------------
    // 5) stripes 对比：同样 workload，stripes 不同，看看吞吐差异（打印即可）
    // -------------------------------
    @Test
    @Timeout(30)
    void perf_compare_stripes() throws Exception {
        int threads = 16;
        int seconds = 3;
        int keySpace = 100_000;

        TestCache c4  = new TestCache(/*capacity*/10_000, /*stripes*/4,   /*value*/256, /*loadMillis*/0);
        TestCache c64 = new TestCache(/*capacity*/10_000, /*stripes*/64,  /*value*/256, /*loadMillis*/0);
        TestCache c256= new TestCache(/*capacity*/10_000, /*stripes*/256, /*value*/256, /*loadMillis*/0);

        KeySupplier uniform = () -> ThreadLocalRandom.current().nextInt(keySpace);

        // 预热（可选）
        for (int i = 0; i < 5_000; i++) {
            long k = uniform.nextKey();
            c4.get(k); c4.release(k);
            c64.get(k); c64.release(k);
            c256.get(k); c256.release(k);
        }

        runBenchmark("[STRIPES] stripes=4",   c4,   threads, seconds, uniform, true);
        runBenchmark("[STRIPES] stripes=64",  c64,  threads, seconds, uniform, true);
        runBenchmark("[STRIPES] stripes=256", c256, threads, seconds, uniform, true);

        // 不做断言：机器/环境差异会影响绝对值，你主要看相对趋势
    }
}
