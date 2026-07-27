package com.minisql.engine.database;

import java.util.concurrent.atomic.AtomicInteger;

import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.table.TableManager;
import com.minisql.engine.transaction.xid.XidAllocator;

/**
 * 封装单个数据库实例关联的 TM/DM/VM/TBM 组件，
 * 并用引用计数控制其生命周期，支持在多个连接之间复用。
 */
public class DatabaseContext {

    private final String name;
    private final XidAllocator xidAllocator;
    private final PageRecordManager pageRecordManager;
    private final TableManager tbm;

    /** 当前有多少个 Executor 持有这个上下文（连接/会话等） */
    private final AtomicInteger refCount = new AtomicInteger(0);

    DatabaseContext(
            String name,
            XidAllocator xidAllocator,
            PageRecordManager pageRecordManager,
            TableManager tbm
    ) {
        this.name = name;
        this.xidAllocator = xidAllocator;
        this.pageRecordManager = pageRecordManager;
        this.tbm = tbm;
    }

    public String getName() {
        return name;
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
        pageRecordManager.close();
        xidAllocator.close();
        // VersionManager 与 TableManager 没有显式 close，随 TM/DM 生命周期结束
    }
}
