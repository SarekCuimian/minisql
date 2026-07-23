package com.minisql.engine.storage.page;

import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minisql.engine.storage.wal.LogManager;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PageCacheTest {

    static Random random = new SecureRandom();

    @TempDir
    Path tempDir;
    
    @Test
    public void testPageCache() throws Exception {
        LogManager lgm = LogManager.create(basePath("pcacher_simple_test0"));
        PageCache pc = PageCache.create(basePath("pcacher_simple_test0"), PageCache.PAGE_SIZE * 50);
        pc.setLogManager(lgm);
        for(int i = 0 ; i < 100; i ++) {
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            tmp[0] = (byte)i;
            int pgno = pc.newPage(tmp);
            Page pg = pc.getPage(pgno);
            pg.release();
        }
        pc.close();
        lgm.close();

        lgm = LogManager.open(basePath("pcacher_simple_test0"));
        pc = PageCache.open(basePath("pcacher_simple_test0"), PageCache.PAGE_SIZE * 50);
        pc.setLogManager(lgm);
        for(int i = 1; i <= 100; i ++) {
            Page pg = pc.getPage(i);
            assertEquals((byte) i - 1, pg.getBytes()[0]);
            pg.release();
        }
        pc.close();
        lgm.close();

    }

    private PageCache pc1;
    private CountDownLatch cdl1;
    private AtomicInteger noPages1;
    @Test
    public void testPageCacheMultiSimple() throws Exception {
        LogManager lgm1 = LogManager.create(basePath("pcacher_simple_test1"));
        pc1 = PageCache.create(basePath("pcacher_simple_test1"), PageCache.PAGE_SIZE * 50);
        pc1.setLogManager(lgm1);
        cdl1 = new CountDownLatch(200);
        noPages1 = new AtomicInteger(0);
        for(int i = 0; i < 200; i ++) {
            int id = i;
            Runnable r = () -> worker1(id);
            new Thread(r).run();
        }
        cdl1.await();
        pc1.close();
        lgm1.close();
    }

    private void worker1(int id) {
        for(int i = 0; i < 80; i ++) {
            int op = Math.abs(random.nextInt() % 20);
            if(op == 0) {
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                int pgno = pc1.newPage(data);
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                noPages1.incrementAndGet();
                pg.release();
            } else if(op < 20) {
                int mod = noPages1.intValue();
                if(mod == 0) {
                    continue;
                }
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                pg.release();
            }
        }
        cdl1.countDown();
    }


    private PageCache pc2, mpc;
    private LogManager lgm2;
    private CountDownLatch cdl2;
    private AtomicInteger noPages2;
    private Lock lockNew;
    @Test
    public void testPageCacheMulti() throws InterruptedException {
        lgm2 = LogManager.create(basePath("pcacher_multi_test"));
        // This scenario keeps many pages dirty until the background cleaner catches up.
        // Use enough capacity so the test validates page contents rather than transient cache pressure.
        pc2 = PageCache.create(basePath("pcacher_multi_test"), PageCache.PAGE_SIZE * 2048);
        pc2.setLogManager(lgm2);
        mpc = new MockPageCache();
        lockNew = new ReentrantLock();

        cdl2 = new CountDownLatch(30);
        noPages2 = new AtomicInteger(0);

        for(int i = 0; i < 30; i ++) {
            int id = i;
            Runnable r = () -> worker2(id);
            new Thread(r).run();
        }
        cdl2.await();

        pc2.close();
        lgm2.close();
    }

    private void worker2(int id) {
        for(int i = 0; i < 1000; i ++) {
            int op = Math.abs(random.nextInt() % 20);
            if(op == 0) {
                // new page
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                lockNew.lock();
                int pgno = pc2.newPage(data);
                int mpgno = mpc.newPage(data);
                assertEquals(mpgno, pgno);
                lockNew.unlock();
                noPages2.incrementAndGet();
            } else if(op < 10) {
                // check
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                pg.wLock();
                assertArrayEquals(mpg.getBytes(), pg.getBytes());
                pg.wUnlock();
                pg.release();
            } else {
                // update
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                byte[] newData = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                
                pg.wLock();
                long[] lsn = lgm2.log(new byte[] {1});
                pc2.markDirtyPage(pgno, lsn[LogManager.START_LSN_INDEX]);
                mpg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j ++) {
                    mpg.getBytes()[j] = newData[j];
                }
                pg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j ++) {
                    pg.getBytes()[j] = newData[j];
                }
                pg.setPageLsn(lsn[LogManager.END_LSN_INDEX]);
                pg.wUnlock();
                pg.release();
            }
        }
        cdl2.countDown();
    }

    private String basePath(String name) {
        return tempDir.resolve(name).toString();
    }
}
