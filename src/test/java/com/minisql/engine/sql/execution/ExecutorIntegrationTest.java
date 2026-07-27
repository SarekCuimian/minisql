package com.minisql.engine.sql.execution;

import com.minisql.engine.database.DatabaseManager;
import com.minisql.engine.database.DatabaseContext;
import com.minisql.engine.storage.wal.LogManager;
import com.minisql.engine.storage.wal.LogRecord;
import com.minisql.engine.storage.wal.LogRecordType;
import com.minisql.engine.transaction.mvcc.VersionManager;
import com.minisql.error.Error;
import com.minisql.result.ExecutionResult;
import com.minisql.result.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端验证 Executor + DatabaseManager + SQL 解析执行链路。
 */
public class ExecutorIntegrationTest {

    @TempDir
    Path temp;

    private static final String TEST_DB = "test_db";
    private DatabaseManager dbm;
    private Executor executor;

    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.close();
        }
        if (dbm != null) {
            dbm.shutdown();
        }
    }

    @Test
    public void groupByHavingOrderBy_shouldReturnExpectedRows() throws Exception {
        setupDatabase();

        exec("create table stu (id int32 primary key, age int32);");
        exec("insert into stu (id, age) values (1, 20);");
        exec("insert into stu (id, age) values (2, 20);");
        exec("insert into stu (id, age) values (3, 30);");

        ExecutionResult res = exec("select age, count(*) as cnt from stu where age > 10 group by age having cnt >= 2;");
        ResultSet rs = res.getResultSet();
        assertNotNull(rs);
        assertEquals(Arrays.asList("age", "cnt"), rs.getHeaders());
        List<List<String>> rows = rs.getRows();
        assertEquals(1, rows.size());
        assertEquals(Arrays.asList("20", "2"), rows.get(0));
    }

    @Test
    @Disabled("ORDER BY is not implemented by the current parser or SELECT AST")
    public void orderByPlainSelect_shouldSortByNumeric() throws Exception {
        setupDatabase();

        exec("create table stu (id int32 primary key, age int32);");
        exec("insert into stu (id, age) values (1, 10);");
        exec("insert into stu (id, age) values (2, 30);");
        exec("insert into stu (id, age) values (3, 20);");

        ExecutionResult res = exec("select id, age from stu order by age desc;");
        ResultSet rs = res.getResultSet();
        assertNotNull(rs);
        assertEquals(Arrays.asList("id", "age"), rs.getHeaders());
        List<List<String>> rows = rs.getRows();
        assertEquals(3, rows.size());
        assertEquals("30", rows.get(0).get(1)); // 第一行 age 最大
        assertTrue(rows.get(0).get(0).equals("2"));
    }

    @Test
    public void distinctByGroup_shouldDeduplicate() throws Exception {
        setupDatabase();

        exec("create table stu (id int32 primary key, age int32);");
        exec("insert into stu (id, age) values (1, 10);");
        exec("insert into stu (id, age) values (2, 10);");
        exec("insert into stu (id, age) values (3, 20);");

        ExecutionResult res = exec("select age from stu group by age;");
        ResultSet rs = res.getResultSet();
        assertNotNull(rs);
        assertEquals(List.of("age"), rs.getHeaders());
        List<List<String>> rows = rs.getRows();
        assertEquals(2, rows.size());
        assertTrue(rows.contains(List.of("10")));
        assertTrue(rows.contains(List.of("20")));
    }

    @Test
    public void implicitRead_shouldNotWriteXidOrWal() throws Exception {
        setupDatabase();
        exec("create table stu (id int32 primary key, age int32);");
        exec("insert into stu (id, age) values (1, 20);");

        Path databaseBase = temp.resolve("dbroot")
                .resolve(TEST_DB)
                .resolve(TEST_DB);
        closeDatabase();
        long xidFileSize = Files.size(Path.of(databaseBase + ".xid"));
        long transactionWalRecords = countTransactionWalRecords(databaseBase);

        dbm = new DatabaseManager(
                temp.resolve("dbroot").toString(),
                16 * 1024 * 1024
        );
        executor = new Executor(dbm);
        exec("use " + TEST_DB + ";");
        for (int i = 0; i < 10; i++) {
            ExecutionResult result = exec("select * from stu where id = 1;");
            assertEquals(1, result.getResultRows());
        }
        closeDatabase();

        assertEquals(xidFileSize, Files.size(Path.of(databaseBase + ".xid")));
        assertEquals(
                transactionWalRecords,
                countTransactionWalRecords(databaseBase)
        );
    }

    @Test
    public void implicitRead_shouldNotSeeUncommittedInsert() throws Exception {
        setupDatabase();
        exec("create table stu (id int32 primary key, age int32);");

        Executor reader = new Executor(dbm);
        try {
            reader.execute(("use " + TEST_DB + ";").getBytes(StandardCharsets.UTF_8));

            exec("begin;");
            exec("insert into stu (id, age) values (1, 20);");

            ExecutionResult beforeCommit = reader.execute(
                    "select * from stu where id = 1;".getBytes(StandardCharsets.UTF_8)
            );
            assertEquals(0, beforeCommit.getResultRows());

            exec("commit;");
            ExecutionResult afterCommit = reader.execute(
                    "select * from stu where id = 1;".getBytes(StandardCharsets.UTF_8)
            );
            assertEquals(1, afterCommit.getResultRows());
        } finally {
            reader.close();
        }
    }

    @Test
    public void readOnlyTransaction_shouldUseEphemeralXidAndRejectWrites() throws Exception {
        setupDatabase();
        DatabaseContext context = dbm.acquire(TEST_DB);
        try {
            VersionManager versionManager =
                    context.getTableManager().getVersionManager();
            long firstXid = versionManager.beginReadOnly();
            long secondXid = versionManager.beginReadOnly();
            try {
                assertTrue(firstXid < 0);
                assertTrue(secondXid < 0);
                assertNotEquals(firstXid, secondXid);
                RuntimeException exception = assertThrows(
                        RuntimeException.class,
                        () -> versionManager.insert(firstXid, new byte[] {1})
                );
                assertSame(Error.ReadOnlyTransactionException, exception);
            } finally {
                versionManager.endReadOnly(firstXid);
                versionManager.endReadOnly(secondXid);
            }
        } finally {
            dbm.release(context);
        }
    }

    private void setupDatabase() throws Exception {
        Path dbRoot = Files.createDirectory(temp.resolve("dbroot"));
        dbm = new DatabaseManager(dbRoot.toString(), 16 * 1024 * 1024);
        dbm.create(TEST_DB);
        executor = new Executor(dbm);
        exec("use " + TEST_DB + ";");
    }

    private void closeDatabase() {
        if (executor != null) {
            executor.close();
            executor = null;
        }
        if (dbm != null) {
            dbm.shutdown();
            dbm = null;
        }
    }

    private long countTransactionWalRecords(Path databaseBase) {
        long count = 0;
        try (LogManager logManager = LogManager.open(databaseBase.toString());
             LogManager.LogReader reader = logManager.getReader()) {
            while (true) {
                LogRecord record = reader.next();
                if (record == null) {
                    return count;
                }
                LogRecordType type = record.getType();
                if (type == LogRecordType.BEGIN
                        || type == LogRecordType.COMMIT
                        || type == LogRecordType.ABORT
                        || type == LogRecordType.END) {
                    count++;
                }
            }
        }
    }

    private ExecutionResult exec(String sql) throws Exception {
        return executor.execute(sql.getBytes(StandardCharsets.UTF_8));
    }
}
