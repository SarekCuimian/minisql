package com.minisql.engine.sql.execution.bench;

import com.minisql.engine.database.DatabaseManager;
import com.minisql.engine.sql.execution.Executor;
import com.minisql.result.ExecutionResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 全面的性能压测套件
 * 测试数据库在不同负载下的性能表现
 * 默认 @Disabled 防止常规 CI 运行，如需压测手工去掉 @Disabled。
 */
@Disabled("性能压测，手工运行时去掉 @Disabled")
public class ExecutorLoadTest {
    @TempDir
    Path temp;

    private static final String DB_NAME = "bench";
    private DatabaseManager dbm;
    private Executor executor;

    @AfterEach
    public void tearDown() {
        if(executor != null) executor.close();
        if(dbm != null) dbm.shutdown();
    }

    /**
     * 测试1: 纯写入性能测试
     * 测试多线程并发插入的吞吐量
     */
    @Test
    public void testPureInsertLoad() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string, score int32);");

        int threads = 8;
        int insertsPerThread = 2000;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        System.out.println("\n========== 纯写入性能测试 ==========");
        System.out.println("线程数: " + threads);
        System.out.println("每线程插入数: " + insertsPerThread);
        
        Instant begin = Instant.now();
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            Thread worker = new Thread(() -> {
                Executor threadExecutor = new Executor(dbm);
                try {
                    threadExecutor.execute(("use " + DB_NAME + ";").getBytes(StandardCharsets.UTF_8));
                    for (int i = 0; i < insertsPerThread; i++) {
                        int id = threadId * insertsPerThread + i + 1;
                        int age = ThreadLocalRandom.current().nextInt(18, 60);
                        String name = "user" + id;
                        int score = ThreadLocalRandom.current().nextInt(0, 100);
                        threadExecutor.execute(
                            String.format("insert into bench (id, age, name, score) values (%d, %d, '%s', %d);",
                                id, age, name, score).getBytes(StandardCharsets.UTF_8)
                        );
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    threadExecutor.close();
                    latch.countDown();
                }
            }, "insert-" + t);
            worker.start();
        }
        latch.await();
        long elapsedMs = Duration.between(begin, Instant.now()).toMillis();
        
        printResults("纯写入", successCount.get(), errorCount.get(), elapsedMs);
        assertTrue(successCount.get() > 0);
    }

    /**
     * 测试2: 纯读取性能测试
     * 测试多线程并发查询的吞吐量
     */
    @Test
    public void testPureSelectLoad() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string, score int32);");
        
        // 预热数据
        System.out.println("\n预热数据中...");
        for (int i = 1; i <= 5000; i++) {
            int age = ThreadLocalRandom.current().nextInt(18, 60);
            exec(String.format("insert into bench (id, age, name, score) values (%d, %d, 'user%d', %d);",
                i, age, i, ThreadLocalRandom.current().nextInt(0, 100)));
        }
        System.out.println("预热完成！");

        int threads = 8;
        int queriesPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        System.out.println("\n========== 纯读取性能测试 ==========");
        System.out.println("线程数: " + threads);
        System.out.println("每线程查询数: " + queriesPerThread);
        
        Instant begin = Instant.now();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                Executor threadExecutor = new Executor(dbm);
                try {
                    threadExecutor.execute(("use " + DB_NAME + ";").getBytes(StandardCharsets.UTF_8));
                    for (int i = 0; i < queriesPerThread; i++) {
                        int age = ThreadLocalRandom.current().nextInt(18, 60);
                        threadExecutor.execute(
                            ("select count(*) from bench where age = " + age + ";").getBytes(StandardCharsets.UTF_8)
                        );
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    threadExecutor.close();
                    latch.countDown();
                }
            }, "select-" + t);
            worker.start();
        }
        latch.await();
        long elapsedMs = Duration.between(begin, Instant.now()).toMillis();
        
        printResults("纯读取", successCount.get(), errorCount.get(), elapsedMs);
        assertTrue(successCount.get() > 0);
    }

    /**
     * 测试3: 混合读写性能测试（70%读 + 30%写）
     * 模拟真实场景的读写混合负载
     */
    @Test
    public void testMixedReadWriteLoad() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string, score int32);");
        
        // 预热插入
        System.out.println("\n预热数据中...");
        for (int i = 1; i <= 1000; i++) {
            int age = ThreadLocalRandom.current().nextInt(18, 60);
            exec(String.format("insert into bench (id, age, name, score) values (%d, %d, 'user%d', %d);",
                i, age, i, ThreadLocalRandom.current().nextInt(0, 100)));
        }
        System.out.println("预热完成！");

        int threads = 8;
        int opsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger readCount = new AtomicInteger(0);
        AtomicInteger writeCount = new AtomicInteger(0);
        
        System.out.println("\n========== 混合读写性能测试 (70%读 + 30%写) ==========");
        System.out.println("线程数: " + threads);
        System.out.println("每线程操作数: " + opsPerThread);
        
        Instant begin = Instant.now();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                Executor threadExecutor = new Executor(dbm);
                try {
                    threadExecutor.execute(("use " + DB_NAME + ";").getBytes(StandardCharsets.UTF_8));
                    for (int i = 0; i < opsPerThread; i++) {
                        // 70% 读，30% 写
                        if(ThreadLocalRandom.current().nextDouble() < 0.7) {
                            int age = ThreadLocalRandom.current().nextInt(18, 60);
                            threadExecutor.execute(
                                ("select count(*) from bench where age = " + age + ";").getBytes(StandardCharsets.UTF_8)
                            );
                            readCount.incrementAndGet();
                        } else {
                            int id = ThreadLocalRandom.current().nextInt(100000, 900000);
                            int age = ThreadLocalRandom.current().nextInt(18, 60);
                            String name = "user" + id;
                            int score = ThreadLocalRandom.current().nextInt(0, 100);
                            threadExecutor.execute(
                                String.format("insert into bench (id, age, name, score) values (%d, %d, '%s', %d);",
                                    id, age, name, score).getBytes(StandardCharsets.UTF_8)
                            );
                            writeCount.incrementAndGet();
                        }
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    threadExecutor.close();
                    latch.countDown();
                }
            }, "mixed-" + t);
            worker.start();
        }
        latch.await();
        long elapsedMs = Duration.between(begin, Instant.now()).toMillis();
        
        System.out.println("读操作数: " + readCount.get());
        System.out.println("写操作数: " + writeCount.get());
        printResults("混合读写", successCount.get(), errorCount.get(), elapsedMs);
        assertTrue(successCount.get() > 0);
    }

    /**
     * 测试4: 更新操作性能测试
     * 测试多线程并发更新的吞吐量
     */
    @Test
    public void testUpdateLoad() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string, score int32);");
        
        // 预热数据
        System.out.println("\n预热数据中...");
        for (int i = 1; i <= 2000; i++) {
            int age = ThreadLocalRandom.current().nextInt(18, 60);
            exec(String.format("insert into bench (id, age, name, score) values (%d, %d, 'user%d', %d);",
                i, age, i, ThreadLocalRandom.current().nextInt(0, 100)));
        }
        System.out.println("预热完成！");

        int threads = 6;
        int updatesPerThread = 500;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        System.out.println("\n========== 更新操作性能测试 ==========");
        System.out.println("线程数: " + threads);
        System.out.println("每线程更新数: " + updatesPerThread);
        
        Instant begin = Instant.now();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                Executor threadExecutor = new Executor(dbm);
                try {
                    threadExecutor.execute(("use " + DB_NAME + ";").getBytes(StandardCharsets.UTF_8));
                    for (int i = 0; i < updatesPerThread; i++) {
                        int id = ThreadLocalRandom.current().nextInt(1, 2001);
                        int newScore = ThreadLocalRandom.current().nextInt(0, 100);
                        threadExecutor.execute(
                            String.format("update bench set score = %d where id = %d;", newScore, id)
                                .getBytes(StandardCharsets.UTF_8)
                        );
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    threadExecutor.close();
                    latch.countDown();
                }
            }, "update-" + t);
            worker.start();
        }
        latch.await();
        long elapsedMs = Duration.between(begin, Instant.now()).toMillis();
        
        printResults("更新操作", successCount.get(), errorCount.get(), elapsedMs);
        assertTrue(successCount.get() > 0);
    }

    /**
     * 测试5: 删除操作性能测试
     * 测试多线程并发删除的吞吐量
     */
    @Test
    public void testDeleteLoad() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string, score int32);");
        
        // 预热数据
        System.out.println("\n预热数据中...");
        for (int i = 1; i <= 10000; i++) {
            int age = ThreadLocalRandom.current().nextInt(18, 60);
            exec(String.format("insert into bench (id, age, name, score) values (%d, %d, 'user%d', %d);",
                i, age, i, ThreadLocalRandom.current().nextInt(0, 100)));
        }
        System.out.println("预热完成！");

        int threads = 6;
        int deletesPerThread = 300;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        System.out.println("\n========== 删除操作性能测试 ==========");
        System.out.println("线程数: " + threads);
        System.out.println("每线程删除数: " + deletesPerThread);
        
        Instant begin = Instant.now();
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            Thread worker = new Thread(() -> {
                Executor threadExecutor = new Executor(dbm);
                try {
                    threadExecutor.execute(("use " + DB_NAME + ";").getBytes(StandardCharsets.UTF_8));
                    for (int i = 0; i < deletesPerThread; i++) {
                        int id = threadId * deletesPerThread + i + 1;
                        threadExecutor.execute(
                            ("delete from bench where id = " + id + ";").getBytes(StandardCharsets.UTF_8)
                        );
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    threadExecutor.close();
                    latch.countDown();
                }
            }, "delete-" + t);
            worker.start();
        }
        latch.await();
        long elapsedMs = Duration.between(begin, Instant.now()).toMillis();
        
        printResults("删除操作", successCount.get(), errorCount.get(), elapsedMs);
        assertTrue(successCount.get() > 0);
    }

    /**
     * 测试6: 完整CRUD混合测试
     * 测试增删改查混合场景下的性能
     */
    @Test
    public void testFullCRUDLoad() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string, score int32);");
        
        // 预热数据
        System.out.println("\n预热数据中...");
        for (int i = 1; i <= 3000; i++) {
            int age = ThreadLocalRandom.current().nextInt(18, 60);
            exec(String.format("insert into bench (id, age, name, score) values (%d, %d, 'user%d', %d);",
                i, age, i, ThreadLocalRandom.current().nextInt(0, 100)));
        }
        System.out.println("预热完成！");

        int threads = 8;
        int opsPerThread = 800;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger selectCount = new AtomicInteger(0);
        AtomicInteger insertCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger deleteCount = new AtomicInteger(0);
        
        System.out.println("\n========== 完整CRUD混合测试 ==========");
        System.out.println("线程数: " + threads);
        System.out.println("每线程操作数: " + opsPerThread);
        System.out.println("操作比例: 50%查询 + 25%插入 + 15%更新 + 10%删除");
        
        Instant begin = Instant.now();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                Executor threadExecutor = new Executor(dbm);
                try {
                    threadExecutor.execute(("use " + DB_NAME + ";").getBytes(StandardCharsets.UTF_8));
                    for (int i = 0; i < opsPerThread; i++) {
                        double rand = ThreadLocalRandom.current().nextDouble();
                        
                        if (rand < 0.50) {
                            // 50% SELECT
                            int age = ThreadLocalRandom.current().nextInt(18, 60);
                            threadExecutor.execute(
                                ("select * from bench where age = " + age + ";").getBytes(StandardCharsets.UTF_8)
                            );
                            selectCount.incrementAndGet();
                        } else if (rand < 0.75) {
                            // 25% INSERT
                            int id = ThreadLocalRandom.current().nextInt(100000, 900000);
                            int age = ThreadLocalRandom.current().nextInt(18, 60);
                            String name = "user" + id;
                            int score = ThreadLocalRandom.current().nextInt(0, 100);
                            threadExecutor.execute(
                                String.format("insert into bench (id, age, name, score) values (%d, %d, '%s', %d);",
                                    id, age, name, score).getBytes(StandardCharsets.UTF_8)
                            );
                            insertCount.incrementAndGet();
                        } else if (rand < 0.90) {
                            // 15% UPDATE
                            int id = ThreadLocalRandom.current().nextInt(1, 3001);
                            int newScore = ThreadLocalRandom.current().nextInt(0, 100);
                            threadExecutor.execute(
                                String.format("update bench set score = %d where id = %d;", newScore, id)
                                    .getBytes(StandardCharsets.UTF_8)
                            );
                            updateCount.incrementAndGet();
                        } else {
                            // 10% DELETE
                            int id = ThreadLocalRandom.current().nextInt(1, 3001);
                            threadExecutor.execute(
                                ("delete from bench where id = " + id + ";").getBytes(StandardCharsets.UTF_8)
                            );
                            deleteCount.incrementAndGet();
                        }
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    threadExecutor.close();
                    latch.countDown();
                }
            }, "crud-" + t);
            worker.start();
        }
        latch.await();
        long elapsedMs = Duration.between(begin, Instant.now()).toMillis();
        
        System.out.println("SELECT: " + selectCount.get());
        System.out.println("INSERT: " + insertCount.get());
        System.out.println("UPDATE: " + updateCount.get());
        System.out.println("DELETE: " + deleteCount.get());
        printResults("完整CRUD", successCount.get(), errorCount.get(), elapsedMs);
        assertTrue(successCount.get() > 0);
    }

    /**
     * 测试7: 事务压力测试
     * 测试显式事务的性能
     */
    @Test
    public void testExplicitTransactionLoad() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string, score int32);");
        
        int threads = 6;
        int transactionsPerThread = 100;
        int opsPerTransaction = 10;
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        System.out.println("\n========== 事务压力测试 ==========");
        System.out.println("线程数: " + threads);
        System.out.println("每线程事务数: " + transactionsPerThread);
        System.out.println("每事务操作数: " + opsPerTransaction);
        
        Instant begin = Instant.now();
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            Thread worker = new Thread(() -> {
                Executor threadExecutor = new Executor(dbm);
                try {
                    threadExecutor.execute(("use " + DB_NAME + ";").getBytes(StandardCharsets.UTF_8));
                    for (int txn = 0; txn < transactionsPerThread; txn++) {
                        threadExecutor.execute("begin;".getBytes(StandardCharsets.UTF_8));
                        for (int op = 0; op < opsPerTransaction; op++) {
                            int id = threadId * transactionsPerThread * opsPerTransaction + txn * opsPerTransaction + op + 1;
                            int age = ThreadLocalRandom.current().nextInt(18, 60);
                            String name = "txnuser" + id;
                            int score = ThreadLocalRandom.current().nextInt(0, 100);
                            threadExecutor.execute(
                                String.format("insert into bench (id, age, name, score) values (%d, %d, '%s', %d);",
                                    id, age, name, score).getBytes(StandardCharsets.UTF_8)
                            );
                        }
                        threadExecutor.execute("commit;".getBytes(StandardCharsets.UTF_8));
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    threadExecutor.close();
                    latch.countDown();
                }
            }, "txn-" + t);
            worker.start();
        }
        latch.await();
        long elapsedMs = Duration.between(begin, Instant.now()).toMillis();
        
        printResults("事务", successCount.get(), errorCount.get(), elapsedMs);
        assertTrue(successCount.get() > 0);
    }

    // ============= 辅助方法 =============
    
    private void setup() throws Exception {
        Path dbRoot = Files.createDirectory(temp.resolve("loaddb"));
        dbm = new DatabaseManager(dbRoot.toString(), 64 * 1024 * 1024);
        dbm.create(DB_NAME);
        executor = new Executor(dbm);
        exec("use " + DB_NAME + ";");
    }

    private ExecutionResult exec(String sql) throws Exception {
        return executor.execute(sql.getBytes(StandardCharsets.UTF_8));
    }

    private void printResults(String testName, int success, int error, long elapsedMs) {
        System.out.println("\n---------- " + testName + "测试结果 ----------");
        System.out.println("成功操作数: " + success);
        System.out.println("失败操作数: " + error);
        System.out.println("总耗时: " + elapsedMs + " ms");
        if (elapsedMs > 0) {
            System.out.println("吞吐量: " + (success * 1000L / elapsedMs) + " ops/sec");
            System.out.println("平均延迟: " + String.format("%.2f", elapsedMs * 1.0 / success) + " ms/op");
        }
        System.out.println("成功率: " + String.format("%.2f", success * 100.0 / (success + error)) + "%");
        System.out.println("========================================\n");
    }
}
