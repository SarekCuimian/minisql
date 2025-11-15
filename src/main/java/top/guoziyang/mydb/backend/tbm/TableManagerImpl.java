package top.guoziyang.mydb.backend.tbm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.parser.statement.Begin;
import top.guoziyang.mydb.backend.parser.statement.Create;
import top.guoziyang.mydb.backend.parser.statement.Delete;
import top.guoziyang.mydb.backend.parser.statement.Describe;
import top.guoziyang.mydb.backend.parser.statement.Insert;
import top.guoziyang.mydb.backend.parser.statement.Select;
import top.guoziyang.mydb.backend.parser.statement.Show;
import top.guoziyang.mydb.backend.parser.statement.Update;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.utils.format.TextTableFormatter;
import top.guoziyang.mydb.backend.vm.IsolationLevel;
import top.guoziyang.mydb.backend.vm.VersionManager;
import top.guoziyang.mydb.common.Error;

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
        return Parser.parseLong(raw);
    }

    /**
     * 更新第一个表的uid
     * @param uid
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        IsolationLevel level = begin.isolationLevel == null ? IsolationLevel.READ_COMMITTED : begin.isolationLevel;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }
    @Override
    public byte[] show(long xid, Show show) {
        if(show == null || show.target == Show.Target.TABLES) {
            return showTables(xid);
        }
        return showDatabases();
    }
    @Override
    public byte[] describe(long xid, Describe describe) throws Exception {
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
        SelectStats.setRowCount(rows.size());
        return TextTableFormatter.format(headers, rows).getBytes();
    }
    @Override
    public byte[] create(long xid, Create create) throws Exception {
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
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    /**
     * 显示所有表
     * @param xid
     * @return
     */
    private byte[] showTables(long xid) {
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
        SelectStats.setRowCount(names.size());
        String header = "Tables_in_" + currentDatabaseName();
        return TextTableFormatter.formatSingleColumn(header, names).getBytes();
    }

    /**
     * 显示所有数据库
     * @return
     */
    private byte[] showDatabases() {
        List<String> databases = new ArrayList<>();
        databases.add(currentDatabaseName());
        SelectStats.setRowCount(databases.size());
        return TextTableFormatter.formatSingleColumn("Database", databases).getBytes();
    }
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
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
