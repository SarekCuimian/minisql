package com.minisql.backend.dbm;

import java.util.concurrent.atomic.AtomicInteger;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.tbm.TableManager;
import com.minisql.backend.txm.TransactionManager;
import com.minisql.backend.vm.VersionManager;

/**
 * 封装单个数据库实例关联的 TM/DM/VM/TBM 组件，
 * 并用引用计数控制其生命周期，支持在多个连接之间复用。
 */
public class DatabaseContext {

    private final String name;
    private final String basePath;

    private final TransactionManager txm;
    private final DataManager dm;
    private final VersionManager vm;
    private final TableManager tbm;

    /** 当前有多少个 Executor 持有这个上下文（连接/会话等） */
    private final AtomicInteger refCount = new AtomicInteger(0);

    DatabaseContext(String name, String basePath,
                    TransactionManager txm, DataManager dm, VersionManager vm, TableManager tbm) {
        this.name = name;
        this.basePath = basePath;
        this.txm = txm;
        this.dm = dm;
        this.vm = vm;
        this.tbm = tbm;
    }

    public String getName() {
        return name;
    }

    public String getBasePath() {
        return basePath;
    }

    public TransactionManager getTransactionManager() {
        return txm;
    }

    public DataManager getDataManager() {
        return dm;
    }

    public VersionManager getVersionManager() {
        return vm;
    }

    public TableManager getTableManager() {
        return tbm;
    }

    /** 增加一次引用：某个连接/会话开始使用这个数据库实例 */
    public void retain() {
        refCount.incrementAndGet();
    }

    /** 释放一次引用：连接/会话结束使用 */
    public void release() {
        int count = refCount.decrementAndGet();
        if (count < 0) {
            // 防御性处理，避免误用导致计数变成负数
            refCount.set(0);
        }
    }

    /** 是否仍有连接在使用这个数据库实例 */
    public boolean inUse() {
        return refCount.get() > 0;
    }

    /**
     * 关闭底层组件，仅应由 DatabaseManager 在确定不再使用该实例时调用。
     * 普通调用方不要直接调用 close()。
     */
    public void close() {
        txm.close();
        dm.close();
        // VersionManager 与 TableManager 没有显式 close，随 TM/DM 生命周期结束
    }
}
