package com.minisql.api.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.minisql.result.format.ConsoleResultFormatter;
import com.minisql.result.ExecutionResult;
import com.minisql.api.dto.ResponseFormat;
import com.minisql.api.dto.SqlExecutionResultDto;
import com.minisql.api.dto.response.SqlExecutionResponse;
import com.minisql.api.session.MiniSqlSession;
import com.minisql.api.session.SessionManager;

import java.nio.charset.StandardCharsets;

@Service
public class SqlService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlService.class);

    private final SessionManager sessionRegistry;
    private final ConsoleResultFormatter formatter =
            new ConsoleResultFormatter();

    public SqlService(SessionManager sessionManager) {
        this.sessionRegistry = sessionManager;
    }

    public SqlExecutionResponse<?> executeWithSession(String sessionId, String sql, ResponseFormat format) {
        MiniSqlSession session = sessionRegistry.getSession(sessionId);
        return doExecute(session, sql, format);
    }

    private SqlExecutionResponse<?> doExecute(MiniSqlSession session, String sql, ResponseFormat format) {
        try {
            ExecutionResult result = session.execute(sql);
            // 返回文本化结果
            if(format == ResponseFormat.TEXT) {
                String text = new String(formatter.format(result), StandardCharsets.UTF_8);
                return SqlExecutionResponse.success(text);
            }
            // 返回扁平化的结构化结果，避免重复字段
            return SqlExecutionResponse.success(SqlExecutionResultDto.from(result));
        } catch (Exception ex) {
            // 捕获所有异常，返回错误信息
            LOGGER.error("执行 SQL 失败: {}", sql, ex);
            return SqlExecutionResponse.failure(ex.getMessage());
        }
    }
}
