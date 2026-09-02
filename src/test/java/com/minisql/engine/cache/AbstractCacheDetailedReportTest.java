package com.minisql.engine.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractCacheDetailedReportTest {

    // -----------------------------
    // 可控子类：统计 loads + 可配置 load 延迟
    // -----------------------------
    static class TestCache extends AbstractCache<byte[]> {
        final int valueSizeBytes;
        final int loadMillis;
        final AtomicLong loads = new AtomicLong();

        TestCache(int capacity, int valueSizeBytes, int loadMillis) {
            super(capacity);
            this.valueSizeBytes = valueSizeBytes;
            this.loadMillis = loadMillis;
        }

        @Override
        protected byte[] loadCache(long key) throws Exception {
            loads.incrementAndGet();
            if (loadMillis > 0) Thread.sleep(loadMillis);
            return new byte[valueSizeBytes];
        }

        @Override
        protected void flushCache(byte[] obj) {
            // no-op
        }
    }

    interface KeySupplier { long nextKey(); }

    // -----------------------------
    // 延迟采样器：线程内采样 + 汇总算分位数
    // -----------------------------
    static class LatencyRecorder {
        private final long[] samples;
        private int idx = 0;

        LatencyRecorder(int maxSamples) {
            this.samples = new long[maxSamples];
        }

        void record(long ns) {
            if (idx < samples.length) samples[idx++] = ns;
        }

        long[] snapshot() {
            return Arrays.copyOf(samples, idx);
        }
    }

    static class Percentiles {
        final long p50, p95, p99, max;
        Percentiles(long p50, long p95, long p99, long max) {
            this.p50 = p50; this.p95 = p95; this.p99 = p99; this.max = max;
        }
    }

    static Percentiles percentiles(long[] arr) {
        if (arr.length == 0) return new Percentiles(0,0,0,0);
        Arrays.sort(arr);
        long p50 = arr[(int)Math.floor(0.50 * (arr.length - 1))];
        long p95 = arr[(int)Math.floor(0.95 * (arr.length - 1))];
        long p99 = arr[(int)Math.floor(0.99 * (arr.length - 1))];
        long max = arr[arr.length - 1];
        return new Percentiles(p50, p95, p99, max);
    }

    static String fmtNs(long ns) {
        if (ns < 1_000) return ns + "ns";
        if (ns < 1_000_000) return String.format("%.2fµs", ns / 1_000.0);
        if (ns < 1_000_000_000) return String.format("%.2fms", ns / 1_000_000.0);
        return String.format("%.2fs", ns / 1_000_000_000.0);
    }

    static class Report {
        final String name;
        final int threads, seconds;
        final long ops;
        final double qps;
        final long loads;
        final long exceptions;
        final Percentiles getLat;

        Report(String name, int threads, int seconds,
               long ops, double qps, long loads, long exceptions, Percentiles getLat) {
            this.name = name;
            this.threads = threads;
            this.seconds = seconds;
            this.ops = ops;
            this.qps = qps;
            this.loads = loads;
            this.exceptions = exceptions;
            this.getLat = getLat;
        }

        void print() {
            System.out.println("========================================");
            System.out.println("SCENARIO: " + name);
            System.out.println("threads=" + threads + ", duration=" + seconds + "s");
            System.out.println("ops=" + ops + ", qps=" + String.format("%.0f", qps));
            System.out.println("loads=" + loads + ", exceptions=" + exceptions);
            System.out.println("getLatency: p50=" + fmtNs(getLat.p50)
                    + ", p95=" + fmtNs(getLat.p95)
                    + ", p99=" + fmtNs(getLat.p99)
                    + ", max=" + fmtNs(getLat.max));
            System.out.println("========================================");
        }
    }

    private static Report runScenario(
            String name,
            TestCache cache,
            int threads,
            int seconds,
            KeySupplier keys,
            boolean doRelease,
            int maxSamplesPerThread
    ) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        LongAdder ops = new LongAdder();
        LongAdder exceptions = new LongAdder();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        LatencyRecorder[] recs = new LatencyRecorder[threads];

        long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            recs[t] = new LatencyRecorder(maxSamplesPerThread);

            pool.submit(() -> {
                try {
                    start.await();
                    while (System.nanoTime() < endAt) {
                        long k = keys.nextKey();
                        long t0 = System.nanoTime();
                        try {
                            byte[] v = cache.get(k);
                            if (v == null) throw new AssertionError("null value");
                            if (doRelease) cache.release(k);
                            ops.increment();
                        } catch (Throwable e) {
                            exceptions.increment();
                            firstFailure.compareAndSet(null, e);
                        } finally {
                            long dt = System.nanoTime() - t0;
                            recs[tid].record(dt);
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await();
        pool.shutdownNow();
        pool.awaitTermination(3, TimeUnit.SECONDS);
        if (firstFailure.get() != null) {
            fail("scenario worker failed: " + name, firstFailure.get());
        }

        // 汇总延迟样本
        int total = 0;
        long[][] snaps = new long[threads][];
        for (int i = 0; i < threads; i++) {
            snaps[i] = recs[i].snapshot();
            total += snaps[i].length;
        }
        long[] all = new long[total];
        int p = 0;
        for (long[] s : snaps) {
            System.arraycopy(s, 0, all, p, s.length);
            p += s.length;
        }

        long opsCount = ops.sum();
        double qps = opsCount / (double) seconds;
        Percentiles getLat = percentiles(all);

        return new Report(name, threads, seconds, opsCount, qps, cache.loads.get(), exceptions.sum(), getLat);
    }

    // -----------------------------
    // 测试：输出更详细的报告
    // -----------------------------

    @Test
    @Timeout(90)
    void detailedReport() throws Exception {
        // 建议：先跑一次短预热（不记录样本），减少 JIT 影响
        {
            TestCache warm = new TestCache(20_000, 256, 0);
            Report r = runScenario("[WARMUP] hit-ish", warm, 8, 2,
                    () -> ThreadLocalRandom.current().nextInt(10_000),
                    true, 10_000);
            // 不打印也行
        }

        // 1) HOT：单 key + 慢 load（验证 SingleFlight + 观察尾延迟）
        {
            TestCache cache = new TestCache(128, 1024, 20);
            Report r = runScenario("[HOT] single key, loadMillis=20",
                    cache, 32, 3,
                    () -> 1L,
                    true,
                    200_000);
            // loads 理论应接近 1
            assertTrue(cache.loads.get() <= 2, "loads too high: " + cache.loads.get());
            r.print();
        }

        // 2) IO：每次使用新 key，确保每个操作都触发一次模拟 I/O。
        // 用于观察持续 miss 在不同 I/O 延迟下的吞吐和端到端延迟。
        {
            int[] loadMillisValues = {1, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000};
            for (int loadMillis : loadMillisValues) {
                TestCache cache = new TestCache(64, 256, loadMillis);
                AtomicLong nextKey = new AtomicLong();
                Report report = runScenario("[IO] distinct keys, loadMillis=" + loadMillis,
                        cache, 8, 2,
                        nextKey::getAndIncrement,
                        true,
                        20_000);
                assertEquals(report.ops, report.loads,
                        "every distinct-key operation must perform exactly one simulated load");
                report.print();
            }
        }

        // 3) CHURN：小容量 + 大 keyspace（观察 miss 主导 + 延迟）
        {
            TestCache cache = new TestCache(128, 256, 0);
            Report r = runScenario("[CHURN] cap=128 keySpace=50000",
                    cache, 8, 3,
                    () -> ThreadLocalRandom.current().nextInt(50_000),
                    true,
                    200_000);
            r.print();
        }

        // 4) PARALLEL：不同 key 的高并发加载
        {
            KeySupplier uniform = () -> ThreadLocalRandom.current().nextInt(100_000);
            TestCache cache = new TestCache(10_000, 256, 0);
            Report report = runScenario("[PARALLEL] distinct keys", cache, 16, 3, uniform, true, 150_000);
            report.print();
        }

        // 5) HIT：100% 命中（预热塞满）
        {
            int keySpace = 10_000;
            TestCache cache = new TestCache(keySpace + 10, 256, 0);
            for (int i = 0; i < keySpace; i++) {
                cache.get(i);
                cache.release(i);
            }
            Report r = runScenario("[HIT] all hit keySpace=10000",
                    cache, 8, 3,
                    () -> ThreadLocalRandom.current().nextInt(keySpace),
                    true,
                    200_000);
            r.print();
            // loads 应该是预热时产生的 keySpace 次（压测阶段基本不再增加）
            assertTrue(cache.loads.get() >= keySpace);
        }
    }
}
