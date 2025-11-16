package com.minisql.backend.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.tbm.TableManager;
import com.minisql.backend.tm.TransactionManager;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.vm.VersionManager;
import com.minisql.backend.vm.VersionManagerImpl;
import com.minisql.common.Error;

/**
 * 管理多个数据库实例，负责创建、打开、删除以及复用 TableManager 等组件。
 */
public class DatabaseProvider {

    private static final String DEFAULT_DATABASE = "database";

    private final Path root;
    private final long mem;
    private final Map<String, DatabaseContext> contexts = new ConcurrentHashMap<>();

    public DatabaseProvider(String rootPath, long mem) {
        this.root = Paths.get(rootPath).toAbsolutePath();
        this.mem = mem;
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 如果没有任何数据库，则自动创建默认 database。
     */
    public void ensureDefaultDatabase() {
        if(!listDatabases().isEmpty()) {
            return;
        }
        try {
            createDatabase(DEFAULT_DATABASE);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    /**
     * 创建新数据库（独立的 TM/DM/VM 存储）。
     */
    public synchronized void createDatabase(String name) throws Exception {
        validateDbName(name);
        Path dir = databaseDir(name);
        if(Files.exists(dir)) {
            throw Error.DatabaseExistsException;
        }
        Files.createDirectories(dir);
        String basePath = databaseBasePath(name);
        TransactionManager tm = TransactionManager.create(basePath);
        DataManager dm = DataManager.create(basePath, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(basePath, vm, dm);
        tm.close();
        dm.close();
    }

    /**
     * 删除数据库，要求没有连接正在使用。
     */
    public synchronized void dropDatabase(String name) throws Exception {
        validateDbName(name);
        DatabaseContext ctx = contexts.get(name);
        if(ctx != null && ctx.inUse()) {
            throw Error.DatabaseInUseException;
        }
        if(ctx != null) {
            ctx.close();
            contexts.remove(name);
        }
        Path dir = databaseDir(name);
        if(!Files.exists(dir)) {
            throw Error.DatabaseNotFoundException;
        }
        deleteRecursively(dir);
    }

    /**
     * 获取数据库上下文，如尚未打开则自动打开。
     */
    public DatabaseContext acquire(String name) throws Exception {
        validateDbName(name);
        DatabaseContext ctx = contexts.computeIfAbsent(name, dbName -> {
            try {
                return openContext(dbName);
            } catch (Exception e) {
                Panic.panic(e);
                return null;
            }
        });
        if(ctx == null) {
            throw Error.DatabaseNotFoundException;
        }
        ctx.retain();
        return ctx;
    }

    /**
     * 释放引用。
     */
    public void release(DatabaseContext ctx) {
        if(ctx == null) {
            return;
        }
        ctx.release();
    }

    /**
     * 列出所有数据库名称（字典序）。
     */
    public List<String> listDatabases() {
        if(!Files.exists(root)) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<>();
        try {
            Files.list(root).forEach(path -> {
                if(Files.isDirectory(path)) {
                    String name = path.getFileName().toString();
                    if(Files.exists(path.resolve(name + ".xid"))) {
                        names.add(name);
                    }
                }
            });
        } catch (IOException e) {
            Panic.panic(e);
        }
        names.sort(Comparator.naturalOrder());
        return names;
    }

    /**
     * 关闭所有已打开的数据库上下文。
     */
    public void shutdown() {
        contexts.values().forEach(DatabaseContext::close);
        contexts.clear();
    }

    public String defaultDatabaseName() {
        List<String> dbs = listDatabases();
        return dbs.isEmpty() ? null : dbs.get(0);
    }

    private DatabaseContext openContext(String name) throws Exception {
        Path dir = databaseDir(name);
        if(!Files.exists(dir)) {
            throw Error.DatabaseNotFoundException;
        }
        String basePath = databaseBasePath(name);
        TransactionManager tm = TransactionManager.open(basePath);
        DataManager dm = DataManager.open(basePath, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(basePath, vm, dm);
        return new DatabaseContext(name, basePath, tm, dm, vm, tbm);
    }

    private Path databaseDir(String name) {
        return root.resolve(name);
    }

    private String databaseBasePath(String name) {
        return databaseDir(name).resolve(name).toString();
    }

    private void deleteRecursively(Path dir) throws IOException {
        if(Files.notExists(dir)) {
            return;
        }
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        Panic.panic(e);
                    }
                });
    }

    private void validateDbName(String name) throws Exception {
        if(name == null || name.isEmpty()) {
            throw Error.InvalidCommandException;
        }
        for(char ch : name.toCharArray()) {
            if(!(Character.isLetterOrDigit(ch) || ch == '_' || ch == '-')) {
                throw Error.InvalidCommandException;
            }
        }
    }
}
