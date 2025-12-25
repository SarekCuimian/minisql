package com.minisql.backend.tbm;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.parser.statement.*;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.common.QueryResult;
import com.minisql.backend.vm.VersionManager;

public interface TableManager {
    VersionManager getVersionManager();
    DataManager getDataManager();

    BeginResult begin(Begin begin);
    QueryResult commit(long xid) throws Exception;
    QueryResult abort(long xid);

    QueryResult show(long xid, Show show);
    QueryResult describe(long xid, Describe describe) throws Exception;

    QueryResult drop(long xid, Drop drop) throws Exception;

    QueryResult create(long xid, Create create) throws Exception;

    QueryResult insert(long xid, Insert insert) throws Exception;
    QueryResult read(long xid, Select select) throws Exception;
    QueryResult update(long xid, Update update) throws Exception;
    QueryResult delete(long xid, Delete delete) throws Exception;

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
