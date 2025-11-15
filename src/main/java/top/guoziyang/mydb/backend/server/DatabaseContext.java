package top.guoziyang.mydb.backend.server;

import java.util.concurrent.atomic.AtomicInteger;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tbm.TableManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.vm.VersionManager;

/**
 * 封装单个数据库实例关联的组件，并维护引用计数，便于在多个连接之间复用。
 */
public class DatabaseContext {
    private final String name;
    private final String basePath;
    private final TransactionManager tm;
    private final DataManager dm;
    private final VersionManager vm;
    private final TableManager tbm;
    private final AtomicInteger refCount = new AtomicInteger(0);

    DatabaseContext(String name,
                    String basePath,
                    TransactionManager tm,
                    DataManager dm,
                    VersionManager vm,
                    TableManager tbm) {
        this.name = name;
        this.basePath = basePath;
        this.tm = tm;
        this.dm = dm;
        this.vm = vm;
        this.tbm = tbm;
    }

    String getName() {
        return name;
    }

    String getBasePath() {
        return basePath;
    }

    TableManager tableManager() {
        return tbm;
    }

    void retain() {
        refCount.incrementAndGet();
    }

    void release() {
        int count = refCount.decrementAndGet();
        if(count < 0) {
            refCount.compareAndSet(count, 0);
        }
    }

    boolean inUse() {
        return refCount.get() > 0;
    }

    void close() {
        tm.close();
        dm.close();
        // verionManager 与 tableManager 没有 close，随 dm/tm 关闭即可
    }
}
