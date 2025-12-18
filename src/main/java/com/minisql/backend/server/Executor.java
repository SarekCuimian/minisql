package com.minisql.backend.server;

import java.util.List;

import com.minisql.backend.dbm.DatabaseContext;
import com.minisql.backend.dbm.DatabaseManager;

import org.checkerframework.checker.units.qual.s;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.minisql.backend.parser.Parser;
import com.minisql.backend.parser.statement.Abort;
import com.minisql.backend.parser.statement.Begin;
import com.minisql.backend.parser.statement.Commit;
import com.minisql.backend.parser.statement.Create;

import com.minisql.backend.parser.statement.CreateDatabase;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Describe;
import com.minisql.backend.parser.statement.Drop;
import com.minisql.backend.parser.statement.DropDatabase;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Show;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.parser.statement.Use;
import com.minisql.backend.tbm.BeginResult;
import com.minisql.backend.tbm.TableManager;
import com.minisql.common.ExecResult;
import com.minisql.common.QueryResult;
import com.minisql.common.ResultSet;
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

    private final DatabaseManager databaseManager;
    private DatabaseContext dbContext;
    /** 用于日志的客户端标识（ip:port 或自定义），允许为空 */
    private final String clientId;

    /** 构造 Executor，绑定数据库管理器 */
    public Executor(DatabaseManager databaseManager, String clientId) {
        this.databaseManager = databaseManager;
        this.clientId = clientId;
        this.xid = 0;
        tryUseDefaultDatabase();
    }
    public Executor(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
        this.xid = 0;
        clientId = "default";
        tryUseDefaultDatabase();
    }

    /**
     * 关闭执行器，如果存在未完成的事务则进行异常回滚。
     */
    public void close() {
        try {
            if(xid != 0 && dbContext != null) {
                System.out.println("Abnormal Abort: " + xid);
                dbContext.getTableManager().abort(xid);
            }
        } finally {
            databaseManager.release(dbContext);
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
        String sqlLog = new String(sql);
        sqlLog.replace("\r", "").replace("\n", "");
        LOGGER.info("[client={}] Execute: {}", clientId, sqlLog);

        Object stat = Parser.parse(sql);

        if(Use.class.isInstance(stat)) {
            return handleUse((Use) stat);

        } else if(CreateDatabase.class.isInstance(stat)) {
            return handleCreateDatabase((CreateDatabase) stat);

        } else if(DropDatabase.class.isInstance(stat)) {
            return handleDropDatabase((DropDatabase) stat);

        // BEGIN
        } else if(Begin.class.isInstance(stat)) {
            TableManager tbm = getTableManager();
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            long start = System.nanoTime();
            BeginResult r = tbm.begin((Begin) stat);
            xid = r.xid;
            return ExecResult.from(r.result, resultType(stat), System.nanoTime() - start);

        // COMMIT
        } else if(Commit.class.isInstance(stat)) {
            TableManager tbm = getTableManager();
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            long start = System.nanoTime();
            QueryResult res = tbm.commit(xid);
            xid = 0;
            return ExecResult.from(res, resultType(stat), System.nanoTime() - start);

        // ROLLBACK
        } else if(Abort.class.isInstance(stat)) {
            TableManager tbm = getTableManager();
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            long start = System.nanoTime();
            QueryResult res = tbm.abort(xid);
            xid = 0;
            return ExecResult.from(res, resultType(stat), System.nanoTime() - start);

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
    private ExecResult executeSQL(Object  stat) throws Exception {
        // SHOW DATABASES 不依赖具体 DB，单独处理
        if(stat instanceof Show && ((Show) stat).target == Show.Target.DATABASES) {
            long start = System.nanoTime();
            QueryResult payload = showDatabases();
            return ExecResult.from(payload, resultType(stat), System.nanoTime() - start);
        }

        TableManager tbm = getTableManager();
        boolean tmpTransaction = false;
        Exception e = null;

        // 自动开始临时事务（如果当前不在事务中）
        if(xid == 0) {
            tmpTransaction = true;
            BeginResult r = tbm.begin(new Begin());
            xid = r.xid;
        }

        long start = System.nanoTime();
        try {
            QueryResult res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid, (Show)stat);
            } else if(Describe.class.isInstance(stat)) {
                res = tbm.describe(xid, (Describe)stat);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Drop.class.isInstance(stat)) {
                res = tbm.drop(xid, (Drop)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return ExecResult.from(res, resultType(stat), System.nanoTime() - start);

        } catch(Exception e1) {
            e = e1;
            if(!tmpTransaction && isConcurrentUpdateException(e1)) {
                autoAbortCurrentTransaction();
            }
            throw e;
        } finally {
            // 自动处理临时事务（成功 → commit，失败 → abort）
            if(tmpTransaction) {
                long currentXid = xid;
                if(e != null) {
                    if(currentXid != 0) {
                        tbm.abort(currentXid);
                    }
                } else {
                    tbm.commit(currentXid);
                }
                xid = 0;
            }
        }
    }
    
    /** 判断语句是否属于查询类（SELECT/SHOW/DESCRIBE） */
    private boolean isQueryStatement(Object stat) {
        return Select.class.isInstance(stat) ||
                Show.class.isInstance(stat) ||
                Describe.class.isInstance(stat);
    }

    private ExecResult.Type resultType(Object stat) {
        return isQueryStatement(stat) ? ExecResult.Type.RESULT : ExecResult.Type.OK;
    }

    private ExecResult handleUse(Use use) throws Exception {
        ensureNoTransaction();
        DatabaseContext newCtx = databaseManager.acquire(use.databaseName);
        databaseManager.release(dbContext);
        dbContext = newCtx;
        long start = System.nanoTime();
        QueryResult payload = QueryResult.message("Database changed to " + use.databaseName, 0);
        return ExecResult.from(payload, resultType(use), System.nanoTime() - start);
    }

    private ExecResult handleCreateDatabase(CreateDatabase createDatabase) throws Exception {
        long start = System.nanoTime();
        databaseManager.create(createDatabase.databaseName);
        QueryResult payload = QueryResult.message("create database " + createDatabase.databaseName, 0);
        return ExecResult.from(payload, resultType(createDatabase), System.nanoTime() - start);
    }

    private ExecResult handleDropDatabase(DropDatabase dropDatabase) throws Exception {
        ensureNoTransaction();
        if(dbContext != null && dropDatabase.databaseName.equals(dbContext.getName())) {
            databaseManager.release(dbContext);
            dbContext = null;
        }
        long start = System.nanoTime();
        databaseManager.drop(dropDatabase.databaseName);
        QueryResult payload = QueryResult.message("drop database " + dropDatabase.databaseName, 0);
        return ExecResult.from(payload, resultType(dropDatabase), System.nanoTime() - start);
    }

    private TableManager getTableManager() throws Exception {
        if(dbContext == null) {
            throw Error.NoDatabaseSelectedException;
        }
        return dbContext.getTableManager();
    }

    private void ensureNoTransaction() throws Exception {
        if(xid != 0) {
            throw Error.SwitchDatabaseInTxnException;
        }
    }

    /*
    * 获取数据库列表
     */
    private QueryResult showDatabases() {
        List<String> dbs = databaseManager.show();
        List<List<String>> rows = new java.util.ArrayList<>();
        for (String db : dbs) {
            rows.add(List.of(db));
        }
        return QueryResult.resultSet(new ResultSet(List.of("Database"), rows));
    }

    private void tryUseDefaultDatabase() {
        String defaultDb = databaseManager.defaultDatabaseName();
        if(defaultDb == null) {
            return;
        }
        try {
            dbContext = databaseManager.acquire(defaultDb);
            LOGGER.info("Default database selected: {}", defaultDb);
        } catch (Exception e) {
            LOGGER.warn("Failed to acquire default database {}: {}", defaultDb, e.getMessage());
        }
    }

    private boolean isConcurrentUpdateException(Throwable throwable) {
        while(throwable != null) {
            if(throwable == Error.ConcurrentUpdateException) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    private void autoAbortCurrentTransaction() {
        long currentXid = xid;
        if(currentXid == 0) {
            return;
        }
        try {
            getTableManager().abort(currentXid);
        } catch (Exception abortErr) {
            LOGGER.warn("Auto abort transaction {} failed: {}", currentXid, abortErr.getMessage());
        } finally {
            xid = 0;
        }
    }
}
