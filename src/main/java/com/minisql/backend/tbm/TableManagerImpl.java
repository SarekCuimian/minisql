package com.minisql.backend.tbm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.parser.statement.Begin;
import com.minisql.backend.parser.statement.Create;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Describe;
import com.minisql.backend.parser.statement.Drop;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Show;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.common.QueryResult;
import com.minisql.common.ResultSet;
import com.minisql.backend.vm.IsolationLevel;
import com.minisql.backend.vm.VersionManager;
import com.minisql.common.Error;

public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private final Booter booter;
    private final Map<String, Table> tableCache;
    private final Lock rLock;
    private final Lock wLock;
    
    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        ReadWriteLock rwLock = new ReentrantReadWriteLock();
        rLock = rwLock.readLock();
        wLock = rwLock.writeLock();
        loadTables();
    }

    /**
     * 加载当前库所有表的元数据到内存
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
     * @param uid 表 uid
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = ByteUtil.longToByte(uid);
        booter.update(raw);
    }

    @Override
    public VersionManager getVersionManager() {
        return vm;
    }

    @Override
    public DataManager getDataManager() {
        return dm;
    }

    @Override
    public BeginResult begin(Begin begin) {
        BeginResult res = new BeginResult();
        IsolationLevel level = begin.isolationLevel == null ? IsolationLevel.READ_COMMITTED : begin.isolationLevel;
        res.xid = vm.begin(level);
        res.result = QueryResult.message("begin", 0);
        return res;
    }
    @Override
    public QueryResult commit(long xid) throws Exception {
        vm.commit(xid);
        return QueryResult.message("commit", 0);
    }
    @Override
    public QueryResult abort(long xid) {
        vm.abort(xid);
        return QueryResult.message("abort", 0);
    }
    @Override
    public QueryResult show(long xid, Show show) {
        // SHOW DATABASES 在 Executor 里直接处理，这里统一返回当前库的表列表
        return showTables(xid);
    }
    @Override
    public QueryResult describe(long xid, Describe describe) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(describe.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            List<String> headers = List.of("Field", "Type", "Null", "Key", "Default", "Extra");
            List<List<String>> rows = new ArrayList<>();
            for (Field field : table.fields) {
                List<String> row = new ArrayList<>();
                row.add(field.fieldName);
                row.add(field.getTypeName());
                if(field.isPrimary()) {
                    row.add("NO");
                } else {
                    row.add("YES");
                }
                if(field.isPrimary()) {
                    row.add("PRI");
                } else if(field.isUnique()) {
                    row.add("UNI");
                } else if(field.isIndexed()) {
                    row.add("MUL");
                } else {
                    row.add("");
                }
                row.add("NULL");
                row.add("");
                rows.add(row);
            }
            return QueryResult.resultSet(new ResultSet(headers, rows));
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public QueryResult drop(long xid, Drop drop) throws Exception {
        wLock.lock();
        try {
            // 通过 uid -> Table 映射按链表顺序查找目标表
            Map<Long, Table> uidTableMap = new HashMap<>();
            for (Table tb : tableCache.values()) {
                uidTableMap.put(tb.uid, tb);
            }
            // 获取头节点 uid
            long uid = firstTableUid();
            Table pre = null;
            Table cur = null;
            while(uid != 0) {
                Table tb = uidTableMap.get(uid);
                if(tb == null) {
                    break;
                }
                if(tb.name.equals(drop.tableName)) {
                    cur = tb;
                    break;
                }
                pre = tb;
                uid = tb.nextUid;
            }
            if(cur == null) {
                throw Error.TableNotFoundException;
            }
            long successorUid = cur.nextUid;
            if(pre == null) {
                // 删除头节点，直接更新 booter
                updateFirstTableUid(successorUid);
            } else {
                // 就地覆盖前驱的 nextUid
                byte[] raw = vm.read(xid, pre.uid);
                int pos = ByteUtil.parseString(raw).size; // 跳过表名
                System.arraycopy(ByteUtil.longToByte(successorUid), 0, raw, pos, 8);
                vm.update(xid, pre.uid, raw);
                pre.nextUid = successorUid;
            }

            tableCache.remove(cur.name);
            return QueryResult.message("drop table " + drop.tableName, 0);
        } finally {
            wLock.unlock();
        }
    }
    
    @Override
    public QueryResult create(long xid, Create create) throws Exception {
        wLock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            return QueryResult.message("create " + create.tableName, 0);
        } finally {
            wLock.unlock();
        }
    }
    @Override
    public QueryResult insert(long xid, Insert insert) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(insert.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            table.insert(xid, insert);
            return QueryResult.message("insert", 1);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * 显示所有表
     * @param xid 事务 id
     * @return
     */
    private QueryResult showTables(long xid) {
        LinkedHashSet<String> names = new LinkedHashSet<>();
        rLock.lock();
        try {
            for (Table tb : tableCache.values()) {
                names.add(tb.name);
            }
        } finally {
            rLock.unlock();
        }
        String header = "Tables_in_" + currentDatabaseName();
        List<List<String>> rows = new ArrayList<>();
        for (String name : names) {
            rows.add(List.of(name));
        }
        return QueryResult.resultSet(new ResultSet(List.of(header), rows));
    }

    @Override
    public QueryResult read(long xid, Select read) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(read.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            ResultSet data = table.read(xid, read);
            return QueryResult.resultSet(data);
        } finally {
            rLock.unlock();
        }
    }
    @Override
    public QueryResult update(long xid, Update update) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(update.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            int count = table.update(xid, update);
            return QueryResult.message("update", count);
        } finally {
            rLock.unlock();
        }
    }
    @Override
    public QueryResult delete(long xid, Delete delete) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(delete.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            int count = table.delete(xid, delete);
            return QueryResult.message("delete", count);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * 获取当前数据库名称
     */
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
        return path.substring(idx + 1);
    }
}
