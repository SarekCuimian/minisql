package com.minisql.backend.tbm;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.parser.statement.Begin;
import com.minisql.backend.parser.statement.Create;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Describe;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Show;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.utils.Parser;
import com.minisql.backend.vm.VersionManager;

public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid, Show show);
    byte[] describe(long xid, Describe describe) throws Exception;
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 创建一个表管理器
     * @param path 表管理器的路径
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @return 表管理器
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
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
