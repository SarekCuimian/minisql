package com.minisql.backend.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.minisql.backend.parser.Parser;
import com.minisql.backend.parser.statement.Abort;
import com.minisql.backend.parser.statement.Begin;
import com.minisql.backend.parser.statement.Commit;
import com.minisql.backend.parser.statement.Create;

import com.minisql.backend.parser.statement.CreateDatabase;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Describe;
import com.minisql.backend.parser.statement.DropDatabase;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Show;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.parser.statement.Use;
import com.minisql.backend.tbm.BeginRes;
import com.minisql.backend.tbm.SelectStats;
import com.minisql.backend.tbm.TableManager;
import com.minisql.backend.utils.format.ExecResult;
import com.minisql.backend.utils.format.TextTableFormatter;
import com.minisql.common.Error;

/**
 * Executor 负责接收 SQL 字节流、解析语句、调度 TableManager 执行具体操作，
 * 同时管理事务上下文（xid）并返回结构化的 {@link ExecResult}。
 * <p>
 * 特点：
 * <ul>
 *     <li>支持显式事务（BEGIN/COMMIT/ABORT）</li>
 *     <li>自动包装隐式事务（非事务命令时自动 begin → commit/abort）</li>
 *     <li>产出结构化结果供上层决定如何格式化</li>
 * </ul>
 */
public class Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);

    /** 当前事务 ID，0 表示未处于事务中 */
    private long xid;

    private final DatabaseProvider databaseProvider;
    private DatabaseContext dbContext;

    /** 构造 Executor，绑定数据库提供者 */
    public Executor(DatabaseProvider databaseProvider) {
        this.databaseProvider = databaseProvider;
        this.xid = 0;
        trySelectDefaultDatabase();
    }

    /**
     * 关闭执行器，如果存在未完成的事务则进行异常回滚。
     */
    public void close() {
        try {
            if(xid != 0 && dbContext != null) {
                System.out.println("Abnormal Abort: " + xid);
                dbContext.tableManager().abort(xid);
            }
        } finally {
            databaseProvider.release(dbContext);
            dbContext = null;
        }
    }

    /**
     * 执行一条 SQL 指令（解析 + 执行）。
     *
     * @param sql SQL 命令的字节数组
     * @return 执行结果
     * @throws Exception SQL 执行过程中出现的异常
     */
    public ExecResult execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.parse(sql);

        if(Use.class.isInstance(stat)) {
            return handleUse((Use) stat);

        } else if(CreateDatabase.class.isInstance(stat)) {
            return handleCreateDatabase((CreateDatabase) stat);

        } else if(DropDatabase.class.isInstance(stat)) {
            return handleDropDatabase((DropDatabase) stat);

        // BEGIN
        } else if(Begin.class.isInstance(stat)) {
            TableManager tbm = tableManager();
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            long start = System.nanoTime();
            BeginRes r = tbm.begin((Begin) stat);
            xid = r.xid;
            return finalizeResult(stat, r.result, start);

        // COMMIT
        } else if(Commit.class.isInstance(stat)) {
            TableManager tbm = tableManager();
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            long start = System.nanoTime();
            byte[] res = tbm.commit(xid);
            xid = 0;
            return finalizeResult(stat, res, start);

        // ROLLBACK
        } else if(Abort.class.isInstance(stat)) {
            TableManager tbm = tableManager();
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            long start = System.nanoTime();
            byte[] res = tbm.abort(xid);
            xid = 0;
            return finalizeResult(stat, res, start);

        // 非事务控制语句 → 进入 executeSQL
        } else {
            return executeSQL(stat);
        }
    }

    /**
     * 用于处理除 BEGIN/COMMIT/ABORT 之外的 SQL 语句，并在必要时自动开启和结束临时事务。
     *
     * @param stat 解析后的语法对象
     * @return 执行结果的字节数组
     */
    private ExecResult executeSQL(Object stat) throws Exception {
        // SHOW DATABASES 不依赖具体 DB，单独处理
        if(stat instanceof Show && ((Show) stat).target == Show.Target.DATABASES) {
            long start = System.nanoTime();
            byte[] payload = formatDatabases();
            return finalizeResult(stat, payload, start);
        }

        TableManager tbm = tableManager();
        boolean tmpTransaction = false;
        Exception e = null;

        // 自动开始临时事务（如果当前不在事务中）
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }

        long start = System.nanoTime();
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid, (Show)stat);
            } else if(Describe.class.isInstance(stat)) {
                res = tbm.describe(xid, (Describe)stat);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return finalizeResult(stat, res, start);

        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            // 自动处理临时事务（成功 → commit，失败 → abort）
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }

    /**
     * 格式化 SQL 执行结果，根据是否为查询语句输出 rows 或 affected rows。
     *
     * @param stat SQL 语句类型对象
     * @param payload 执行结果数据
     * @param startTime 执行开始时间（纳秒）
     * @return 格式化的结果文本字节数组
     */
    private ExecResult finalizeResult(Object stat, byte[] payload, long startTime) {
        if(payload == null) {
            payload = new byte[0];
        }
        if(isQueryStatement(stat)) {
            long elapsed = System.nanoTime() - startTime;
            int resultRows = SelectStats.getAndResetRowCount();
            return ExecResult.resultSet(payload, elapsed, resultRows);
        }
        long elapsed = System.nanoTime() - startTime;
        int affectedRows = estimateAffectedRows(stat, payload);
        return ExecResult.okPacket(payload, elapsed, affectedRows);
    }

    /** 判断语句是否属于查询类（SELECT/SHOW/DESCRIBE） */
    private boolean isQueryStatement(Object stat) {
        return Select.class.isInstance(stat) ||
                Show.class.isInstance(stat) ||
                Describe.class.isInstance(stat);
    }

    /**
     * 推算受影响的行数，用于 Insert/Update/Delete 的结果格式化。
     *
     * @param stat SQL 语句对象
     * @param payload 执行结果数据
     * @return 受影响行数
     */
    private int estimateAffectedRows(Object stat, byte[] payload) {
        if(Insert.class.isInstance(stat)) {
            return 1;
        } else if(Update.class.isInstance(stat)) {
            return parseCount(payload, "update");
        } else if(Delete.class.isInstance(stat)) {
            return parseCount(payload, "delete");
        } else if(Create.class.isInstance(stat)) {
            return 0;
        } else if(CreateDatabase.class.isInstance(stat) || DropDatabase.class.isInstance(stat)) {
            return 0;
        } else if(Use.class.isInstance(stat)) {
            return 0;
        } else if(Begin.class.isInstance(stat) || Commit.class.isInstance(stat) || Abort.class.isInstance(stat)) {
            return 0;
        }
        return -1;
    }

    /**
     * 从返回字符串中解析 update/delete 计数。
     */
    private int parseCount(byte[] payload, String prefix) {
        String str = new String(payload, StandardCharsets.UTF_8).trim();
        if(!str.startsWith(prefix)) {
            return -1;
        }
        String[] parts = str.split("\\s+");
        if(parts.length < 2) {
            return -1;
        }
        try {
            return Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private ExecResult handleUse(Use use) throws Exception {
        ensureNoTransaction();
        DatabaseContext newCtx = databaseProvider.acquire(use.databaseName);
        databaseProvider.release(dbContext);
        dbContext = newCtx;
        long start = System.nanoTime();
        byte[] payload = ("Database changed to " + use.databaseName).getBytes(StandardCharsets.UTF_8);
        return finalizeResult(use, payload, start);
    }

    private ExecResult handleCreateDatabase(CreateDatabase createDatabase) throws Exception {
        long start = System.nanoTime();
        databaseProvider.createDatabase(createDatabase.databaseName);
        byte[] payload = ("create database " + createDatabase.databaseName).getBytes(StandardCharsets.UTF_8);
        return finalizeResult(createDatabase, payload, start);
    }

    private ExecResult handleDropDatabase(DropDatabase dropDatabase) throws Exception {
        ensureNoTransaction();
        if(dbContext != null && dropDatabase.databaseName.equals(dbContext.getName())) {
            databaseProvider.release(dbContext);
            dbContext = null;
        }
        long start = System.nanoTime();
        databaseProvider.dropDatabase(dropDatabase.databaseName);
        byte[] payload = ("drop database " + dropDatabase.databaseName).getBytes(StandardCharsets.UTF_8);
        return finalizeResult(dropDatabase, payload, start);
    }

    private TableManager tableManager() throws Exception {
        if(dbContext == null) {
            throw Error.NoDatabaseSelectedException;
        }
        return dbContext.tableManager();
    }

    private void ensureNoTransaction() throws Exception {
        if(xid != 0) {
            throw Error.SwitchDatabaseInTxnException;
        }
    }

    private byte[] formatDatabases() {
        List<String> dbs = databaseProvider.listDatabases();
        SelectStats.setRowCount(dbs.size());
        return TextTableFormatter.formatSingleColumn("Database", dbs).getBytes(StandardCharsets.UTF_8);
    }

    private void trySelectDefaultDatabase() {
        String defaultDb = databaseProvider.defaultDatabaseName();
        if(defaultDb == null) {
            return;
        }
        try {
            dbContext = databaseProvider.acquire(defaultDb);
            LOGGER.info("Default database selected: {}", defaultDb);
        } catch (Exception e) {
            LOGGER.warn("Failed to acquire default database {}: {}", defaultDb, e.getMessage());
        }
    }
}
