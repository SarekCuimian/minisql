package com.minisql.backend.vm;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.minisql.backend.utils.Panic;

import static org.junit.Assert.*;

public class LockManagerTest {

    @Test
    public void testLockTable() {
        LockManager lt = new LockManager();
        try {
            // T1 持有 U1
            CountDownLatch l1 = lt.acquire(1, 1);
            assert l1 == null;
        } catch (Exception e) {
            Panic.of(e);
        }

        try {
            // T2 持有 U2
            CountDownLatch l2 = lt.acquire(2, 2);
            assert l2 == null;
        } catch (Exception e) {
            Panic.of(e);
        }

        try {
            // T2 申请 U1，等待 T1
            CountDownLatch l3 = lt.acquire(2, 1);
            // 这里是否为 null 不影响我们测试“死锁检测”，可以不关心
        } catch (Exception e) {
            Panic.of(e);
        }

        // T1 再申请 U2，形成环：T1 等 T2，T2 等 T1，应当触发死锁异常
        assertThrows(RuntimeException.class, () -> {
            try {
                lt.acquire(1, 2);
            } catch (Exception e) {
                // addRow 抛出的 Error.DeadlockException 一般是 RuntimeException 子类
                throw (RuntimeException) e;
            }
        });
    }

    @Test
    public void testLockTable2() {
        LockManager lt = new LockManager();

        // 1. 每个事务 i 先各自拿到资源 i
        for (long i = 1; i <= 100; i++) {
            try {
                CountDownLatch latch = lt.acquire(i, i);
                // 这里第一次拿锁，一定不需要等待，latch 应为 null
                // 但我们不需要断言 latch 的值，测试目标是后面的死锁
            } catch (Exception e) {
                Panic.of(e);
            }
        }

        // 2. 每个事务 i 再去申请资源 i+1，形成长链等待
        for (long i = 1; i <= 99; i++) {
            try {
                CountDownLatch latch = lt.acquire(i, i + 1);
                // 这里会进入等待队列，addRow 可能返回一个 CountDownLatch
                // 但单测只关心最后是否能检测出死锁，所以也可以忽略 latch
            } catch (Exception e) {
                Panic.of(e);
            }
        }

        // 现在 T1 等 T2，T2 等 T3，...，T99 等 T100
        // 如果此时 T100 再申请 U1，就形成一个大环，应该抛死锁
        assertThrows(RuntimeException.class, () -> {
            try {
                lt.acquire(100, 1);
            } catch (Exception e) {
                throw (RuntimeException) e;
            }
        });

        // 移除其中一个事务，打断环
        lt.clear(23);

        // 再次申请同样的锁应该不再触发死锁
        try {
            CountDownLatch latch = lt.acquire(100, 1);
            // 这里可能需要等待，也可能直接拿到锁，但不会再因为死锁抛异常
        } catch (Exception e) {
            Panic.of(e);
        }
    }

    @Test
    public void testWaitAndWakeupWithDelay() throws Exception {
        LockManager lt = new LockManager();

        System.out.println("=== 测试开始 ===");

        // 1. T1 先拿到 U1
        System.out.println("[主线程] T1 加锁 U1");
        CountDownLatch latch1 = lt.acquire(1L, 1L);
        assertNull(latch1);

        // 2. T2 请求 U1，会进入等待，拿到自己的 latch
        System.out.println("[主线程] T2 尝试加锁 U1，需要等待");
        CountDownLatch waitLatch = lt.acquire(2L, 1L);
        assertNotNull(waitLatch);

        CountDownLatch readyLatch = new CountDownLatch(1);
        CountDownLatch doneLatch  = new CountDownLatch(1);

        Thread worker = new Thread(() -> {
            try {
                System.out.println("[T2] 即将进入 await 阻塞");
                readyLatch.countDown();

                long start = System.currentTimeMillis();
                waitLatch.await();  // 真正阻塞
                long end = System.currentTimeMillis();

                System.out.println("[T2] 已被唤醒! 阻塞时长 = " + (end - start) + " ms");

                doneLatch.countDown();
            } catch (InterruptedException e) {
                Panic.of(e);
            }
        }, "T2-Thread");

        worker.start();

        // 等 T2 确认开始 await
        assertTrue(readyLatch.await(1, TimeUnit.SECONDS));
        System.out.println("[主线程] 已确认 T2 开始 await");

        // 刻意等一会儿再释放，保证 T2 至少阻塞一小段时间
        Thread.sleep(3000);  // 自己调，比如 500ms 或 1s
        System.out.println("[主线程] 调用 lt.remove(1)，释放 U1（延迟后）");
        lt.clear(1L);

        boolean woken = doneLatch.await(2, TimeUnit.SECONDS);
        System.out.println("[主线程] T2 唤醒状态 = " + woken);

        assertTrue("T2 没有在资源释放后被唤醒", woken);

        System.out.println("=== 测试结束 ===");
    }


}
