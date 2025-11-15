package top.guoziyang.mydb.backend.parser.statement;

import top.guoziyang.mydb.backend.vm.IsolationLevel;

public class Begin {
    /**
     * 事务隔离级别，默认 READ COMMITTED
     */
    public IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
}
