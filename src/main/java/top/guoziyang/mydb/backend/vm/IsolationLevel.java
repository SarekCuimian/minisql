package top.guoziyang.mydb.backend.vm;

/**
 * 事务隔离级别
 */
public enum IsolationLevel {
    READ_COMMITTED,
    REPEATABLE_READ;

    public static IsolationLevel defaultLevel() {
        return READ_COMMITTED;
    }
}
