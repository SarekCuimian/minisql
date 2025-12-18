package com.minisql.api.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.minisql.common.ConsoleResultFormatter;
import com.minisql.common.ExecResult;
import com.minisql.common.ResultFormatter;
import com.minisql.api.entity.enums.ResponseFormat;
import com.minisql.api.entity.SqlExecResult;
import com.minisql.api.entity.response.SqlExecResponse;
import com.minisql.api.session.MiniSqlSession;
import com.minisql.api.session.SessionManager;

import java.nio.charset.StandardCharsets;

@Service
public class SqlService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlService.class);

    private final SessionManager sessionRegistry;
    private final ResultFormatter formatter = new ConsoleResultFormatter();

    public SqlService(SessionManager sessionManager) {
        this.sessionRegistry = sessionManager;
    }

    public SqlExecResponse<?> executeWithSession(String sessionId, String sql, ResponseFormat format) {
        MiniSqlSession session = sessionRegistry.getSession(sessionId);
        return doExecute(session, sql, format);
    }

    private SqlExecResponse<?> doExecute(MiniSqlSession session, String sql, ResponseFormat format) {
        try {
            ExecResult result = session.execute(sql);
            // 返回文本化结果
            if(format == ResponseFormat.TEXT) {
                String text = new String(formatter.format(result), StandardCharsets.UTF_8);
                return SqlExecResponse.success(text);
            }
            // 返回扁平化的结构化结果，避免重复字段
            return SqlExecResponse.success(SqlExecResult.from(result));
        } catch (Exception ex) {
            // 捕获所有异常，返回错误信息
            LOGGER.error("执行 SQL 失败: {}", sql, ex);
            return SqlExecResponse.failure(ex.getMessage());
        }
    }
}
