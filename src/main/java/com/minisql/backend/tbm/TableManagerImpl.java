package com.minisql.backend.tbm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.parser.statement.Begin;
import com.minisql.backend.parser.statement.Create;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Describe;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Show;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.common.OpResult;
import com.minisql.common.ResultSet;
import com.minisql.backend.vm.IsolationLevel;
import com.minisql.backend.vm.VersionManager;
import com.minisql.common.Error;

public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;
    
    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    /**
     * 加载所有表
     */
    private void loadTables() {
        long uid = firstTableUid();
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }
    /**
     * 获取第一个表的uid
     */
    private long firstTableUid() {
        byte[] raw = booter.load();
        return ByteUtil.parseLong(raw);
    }

    /**
     * 更新第一个表的uid
     * @param uid
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = ByteUtil.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        IsolationLevel level = begin.isolationLevel == null ? IsolationLevel.READ_COMMITTED : begin.isolationLevel;
        res.xid = vm.begin(level);
        res.result = OpResult.message("begin", 0);
        return res;
    }
    @Override
    public OpResult commit(long xid) throws Exception {
        vm.commit(xid);
        return OpResult.message("commit", 0);
    }
    @Override
    public OpResult abort(long xid) {
        vm.abort(xid);
        return OpResult.message("abort", 0);
    }
    @Override
    public OpResult show(long xid, Show show) {
        // SHOW DATABASES 在 Executor 里直接处理，这里统一返回当前库的表列表
        return showTables(xid);
    }
    @Override
    public OpResult describe(long xid, Describe describe) throws Exception {
        Table table;
        lock.lock();
        try {
            table = tableCache.get(describe.tableName);
            if(table == null) {
                List<Table> pending = xidTableCache.get(xid);
                if(pending != null) {
                    for (Table tb : pending) {
                        if(tb.name.equals(describe.tableName)) {
                            table = tb;
                            break;
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        List<String> headers = List.of("Field", "Type", "Null", "Key", "Default", "Extra");
        List<List<String>> rows = new ArrayList<>();
        for (Field field : table.fields) {
            List<String> row = new ArrayList<>();
            row.add(field.fieldName);
            row.add(field.fieldType);
            row.add("YES");
            if(field.isUnique()) {
                row.add("PRI");
            } else if(field.isIndexed()) {
                row.add("MUL");
            } else {
                row.add("");
            }
            row.add("NULL");
            row.add("");
            rows.add(row);
        }
        return OpResult.resultSet(new ResultSet(headers, rows));
    }
    @Override
    public OpResult create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return OpResult.message("create " + create.tableName, 0);
        } finally {
            lock.unlock();
        }
    }
    @Override
    public OpResult insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return OpResult.message("insert", 1);
    }

    /**
     * 显示所有表
     * @param xid
     * @return
     */
    private OpResult showTables(long xid) {
        lock.lock();
        List<String> names = new ArrayList<>();
        try {
            for (Table tb : tableCache.values()) {
                names.add(tb.name);
            }
            List<Table> pending = xidTableCache.get(xid);
            if(pending != null) {
                for (Table tb : pending) {
                    names.add(tb.name);
                }
            }
        } finally {
            lock.unlock();
        }
        String header = "Tables_in_" + currentDatabaseName();
        List<List<String>> rows = new ArrayList<>();
        for (String name : names) {
            rows.add(List.of(name));
        }
        return OpResult.resultSet(new ResultSet(List.of(header), rows));
    }

    @Override
    public OpResult read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        ResultSet data = table.read(xid, read);
        return OpResult.resultSet(data);
    }
    @Override
    public OpResult update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return OpResult.message("update", count);
    }
    @Override
    public OpResult delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return OpResult.message("delete", count);
    }

    private String currentDatabaseName() {
        String path = booter.getPath();
        if(path == null || path.isEmpty()) {
            return "database";
        }
        path = path.replace('\\', '/');
        if(path.endsWith("/")) {
            path = path.substring(0, path.length()-1);
        }
        int idx = path.lastIndexOf('/');
        if(idx == -1) {
            return path;
        }
        return path.substring(idx+1);
    }
}
