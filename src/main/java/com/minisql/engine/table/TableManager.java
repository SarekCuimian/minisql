package com.minisql.engine.table;

import com.minisql.engine.storage.DataManager;
import com.minisql.engine.sql.ast.statement.*;
import com.minisql.engine.storage.codec.ByteUtil;
import com.minisql.result.StatementResult;
import com.minisql.engine.transaction.mvcc.VersionManager;

public interface TableManager {
    VersionManager getVersionManager();
    DataManager getDataManager();

    long begin(Begin begin);
    void commit(long xid) throws Exception;
    void abort(long xid);

    StatementResult show(long xid, Show show);
    StatementResult describe(long xid, Describe describe) throws Exception;

    StatementResult drop(long xid, Drop drop) throws Exception;

    StatementResult create(long xid, Create create) throws Exception;

    StatementResult insert(long xid, Insert insert) throws Exception;
    StatementResult read(long xid, Select select) throws Exception;
    StatementResult update(long xid, Update update) throws Exception;
    StatementResult delete(long xid, Delete delete) throws Exception;

    /**
     * 创建一个表管理器
     * @param path 表管理器的路径
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @return 表管理器
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(ByteUtil.longToByte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * 打开一个表管理器
     * @param path 表管理器的路径
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @return 表管理器
     */
    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
