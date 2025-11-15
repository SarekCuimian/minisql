package top.guoziyang.mydb.web.session;

import top.guoziyang.mydb.backend.utils.format.ExecResult;

public interface MiniSqlSession {
    ExecResult execute(String sql) throws Exception;
    void close();
}
