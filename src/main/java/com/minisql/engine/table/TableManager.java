package com.minisql.engine.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.sql.ast.statement.Begin;
import com.minisql.engine.sql.ast.statement.Create;
import com.minisql.engine.sql.ast.statement.Delete;
import com.minisql.engine.sql.ast.statement.Describe;
import com.minisql.engine.sql.ast.statement.Drop;
import com.minisql.engine.sql.ast.statement.Insert;
import com.minisql.engine.sql.ast.statement.Select;
import com.minisql.engine.sql.ast.statement.Show;
import com.minisql.engine.sql.ast.statement.Update;
import com.minisql.engine.storage.codec.ByteReader;
import com.minisql.engine.storage.codec.ByteUtil;
import com.minisql.result.StatementResult;
import com.minisql.result.ResultSet;
import com.minisql.engine.transaction.mvcc.IsolationLevel;
import com.minisql.engine.transaction.mvcc.ReadView;
import com.minisql.engine.transaction.mvcc.VersionManager;
import com.minisql.error.Error;

public final class TableManager {
    VersionManager vm;
    PageRecordManager pageRecordManager;
    private final Booter booter;
    private final Map<String, Table> tableCache;
    private final Lock rLock;
    private final Lock wLock;

    private TableManager(VersionManager vm, PageRecordManager pageRecordManager, Booter booter) {
        this.vm = vm;
        this.pageRecordManager = pageRecordManager;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        ReadWriteLock rwLock = new ReentrantReadWriteLock();
        rLock = rwLock.readLock();
        wLock = rwLock.writeLock();
        loadTables();
    }

    public static TableManager create(String path, VersionManager versionManager, PageRecordManager pageRecordManager) {
        Booter booter = Booter.create(path);
        byte[] firstTableUid = new byte[Long.BYTES];
        ByteUtil.putLong(firstTableUid, 0, 0);
        booter.update(firstTableUid);
        return new TableManager(versionManager, pageRecordManager, booter);
    }

    public static TableManager open(String path, VersionManager versionManager, PageRecordManager pageRecordManager) {
        return new TableManager(
                versionManager,
                pageRecordManager,
                Booter.open(path)
        );
    }

    /**
     * 加载当前库所有表的元数据到内存
     */
    private void loadTables() {
        long uid = firstTableUid();
        while(uid != 0) {
            Table tb = Table.load(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }
    /**
     * 获取第一个表的uid
     */
    private long firstTableUid() {
        byte[] bootBytes = booter.load();
        return ByteUtil.getLong(bootBytes, 0);
    }

    /**
     * 更新第一个表的uid
     * @param uid 表 uid
     */
    private void updateFirstTableUid(long uid) {
        byte[] bootBytes = new byte[Long.BYTES];
        ByteUtil.putLong(bootBytes, 0, uid);
        booter.update(bootBytes);
    }
    public VersionManager getVersionManager() {
        return vm;
    }
    public PageRecordManager getPageRecordManager() {
        return pageRecordManager;
    }
    public long begin(Begin begin) {
        IsolationLevel level = begin.isolationLevel == null ? IsolationLevel.READ_COMMITTED : begin.isolationLevel;
        return vm.begin(level);
    }
    public long beginReadOnly() {
        return vm.beginReadOnly();
    }
    public void endReadOnly(long xid) throws Exception {
        vm.endReadOnly(xid);
    }
    public void commit(long xid) throws Exception {
        vm.commit(xid);
    }
    public void abort(long xid) {
        vm.abort(xid);
    }
    public StatementResult show(long xid, Show show) {
        // SHOW DATABASES 在 Executor 里直接处理，这里统一返回当前库的表列表
        return showTables(xid);
    }
    public StatementResult describe(long xid, Describe describe) throws Exception {
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
            return StatementResult.resultSet(new ResultSet(headers, rows));
        } finally {
            rLock.unlock();
        }
    }
    public StatementResult drop(long xid, Drop drop) throws Exception {
        wLock.lock();
        try {
            ReadView readView = vm.openReadView(xid);
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
                byte[] tableBytes = vm.read(
                        xid,
                        readView,
                        pre.uid
                );
                ByteReader reader = ByteReader.wrap(tableBytes);
                int tableNameByteLength = reader.readInt();
                reader.skip(tableNameByteLength);
                ByteUtil.putLong(tableBytes, reader.position(), successorUid);
                vm.update(xid, pre.uid, tableBytes);
                pre.nextUid = successorUid;
            }

            tableCache.remove(cur.name);
            return StatementResult.message("drop table " + drop.tableName, 0);
        } finally {
            wLock.unlock();
        }
    }
    public StatementResult create(long xid, Create create) throws Exception {
        wLock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            return StatementResult.message("create " + create.tableName, 0);
        } finally {
            wLock.unlock();
        }
    }
    public StatementResult insert(long xid, Insert insert) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(insert.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            ReadView readView = vm.openReadView(xid);
            table.insert(xid, readView, insert);
            return StatementResult.message("insert", 1);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * 显示所有表
     * @param xid 事务 id
     * @return
     */
    private StatementResult showTables(long xid) {
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
        return StatementResult.resultSet(new ResultSet(List.of(header), rows));
    }
    public StatementResult read(long xid, Select read) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(read.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            ReadView readView = vm.openReadView(xid);
            ResultSet data = table.read(xid, readView, read);
            return StatementResult.resultSet(data);
        } finally {
            rLock.unlock();
        }
    }
    public StatementResult update(long xid, Update update) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(update.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            ReadView readView = vm.openReadView(xid);
            int count = table.update(xid, readView, update);
            return StatementResult.message("update", count);
        } finally {
            rLock.unlock();
        }
    }
    public StatementResult delete(long xid, Delete delete) throws Exception {
        rLock.lock();
        try {
            Table table = tableCache.get(delete.tableName);
            if(table == null) {
                throw Error.TableNotFoundException;
            }
            ReadView readView = vm.openReadView(xid);
            int count = table.delete(xid, readView, delete);
            return StatementResult.message("delete", count);
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
