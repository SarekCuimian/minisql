package com.minisql.engine.sql.execution.bench;

import com.minisql.engine.database.DatabaseManager;
import com.minisql.engine.sql.execution.Executor;
import com.minisql.result.ExecutionResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 简单性能冒烟：在临时库中批量插入 + 查询，输出吞吐/耗时，便于手工比对。
 * 不是严格基准，重在可运行和可观察。
 */
public class ExecutorPerfSmokeTest {

    @TempDir
    Path temp;

    private DatabaseManager dbm;
    private Executor executor;

    @AfterEach
    public void tearDown() {
        if(executor != null) executor.close();
        if(dbm != null) dbm.shutdown();
    }

    @Test
    public void bulkInsertAndQuery() throws Exception {
        setup();
        exec("create table bench (id int32 primary key, age int32, name string);");

        int total = 1000;
        Instant begin = Instant.now();
        for (int i = 1; i <= total; i++) {
            int age = ThreadLocalRandom.current().nextInt(18, 60);
            String name = "u" + i;
            exec(String.format("insert into bench (id, age, name) values (%d, %d, '%s');", i, age, name));
        }
        long autoCommitInsertMs = Duration.between(begin, Instant.now()).toMillis();

        begin = Instant.now();
        ExecutionResult res = exec("select age, count(*) as cnt from bench where age > 20 group by age;");
        long queryMs = Duration.between(begin, Instant.now()).toMillis();

        int pointReadCount = 2000;
        begin = Instant.now();
        for (int i = 0; i < pointReadCount; i++) {
            int id = ThreadLocalRandom.current().nextInt(1, total + 1);
            ExecutionResult pointRead = exec(
                    "select * from bench where id = " + id + ";"
            );
            assertTrue(pointRead.getResultRows() == 1);
        }
        long pointReadMs = Duration.between(begin, Instant.now()).toMillis();

        exec("create table batch_bench (id int32 primary key, value int32);");
        begin = Instant.now();
        exec("begin;");
        for (int i = 1; i <= total; i++) {
            exec(String.format(
                    "insert into batch_bench (id, value) values (%d, %d);",
                    i,
                    i
            ));
        }
        exec("commit;");
        long batchInsertMs = Duration.between(begin, Instant.now()).toMillis();

        System.out.println(
                "Auto-commit inserted " + total + " rows in "
                        + autoCommitInsertMs + " ms"
        );
        System.out.println(
                "Transaction batch inserted " + total + " rows in "
                        + batchInsertMs + " ms"
        );
        System.out.println(
                "Primary-key point reads " + pointReadCount + " times in "
                        + pointReadMs + " ms"
        );
        System.out.println("Group query in " + queryMs + " ms, result rows=" + res.getResultRows());

        assertTrue(res.getResultRows() >= 0); // 仅确保执行成功
    }

    private void setup() throws Exception {
        Path dbRoot = Files.createDirectory(temp.resolve("perfdb"));
        dbm = new DatabaseManager(dbRoot.toString(), 32 * 1024 * 1024);
        String dbName = "perf";
        dbm.create(dbName);
        executor = new Executor(dbm);
        exec("use " + dbName + ";");
    }

    private ExecutionResult exec(String sql) throws Exception {
        return executor.execute(sql.getBytes(StandardCharsets.UTF_8));
    }
}
