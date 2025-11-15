package top.guoziyang.mydb.web.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import top.guoziyang.mydb.backend.utils.format.ConsoleResultFormatter;
import top.guoziyang.mydb.backend.utils.format.ExecResult;
import top.guoziyang.mydb.backend.utils.format.ResultFormatter;
import top.guoziyang.mydb.web.dto.SqlExecResponse;
import top.guoziyang.mydb.web.session.MiniSqlSession;
import top.guoziyang.mydb.web.session.SessionManager;

import java.nio.charset.StandardCharsets;

@Service
public class SqlService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlService.class);

    private final SessionManager sessionRegistry;
    private final ResultFormatter formatter = new ConsoleResultFormatter();

    public SqlService(SessionManager sessionManager) {
        this.sessionRegistry = sessionManager;
    }

    public SqlExecResponse executeWithSession(String sessionId, String sql) {
        MiniSqlSession session = sessionRegistry.getRequiredSession(sessionId);
        return doExecute(session, sql);
    }

    private SqlExecResponse doExecute(MiniSqlSession session, String sql) {
        try {
            ExecResult result = session.execute(sql);
            byte[] formatted = formatter.format(result);
            return SqlExecResponse.success(new String(formatted, StandardCharsets.UTF_8));
        } catch (Exception ex) {
            LOGGER.error("执行 SQL 失败: {}", sql, ex);
            return SqlExecResponse.failure(ex.getMessage());
        }
    }
}
