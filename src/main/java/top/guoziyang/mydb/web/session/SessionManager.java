package top.guoziyang.mydb.web.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;
import top.guoziyang.mydb.web.config.MiniSqlProperties;
import top.guoziyang.mydb.web.exception.SessionNotFoundException;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 简易 session 注册表，负责创建、缓存及关闭多个 MiniSql 会话。
 */
@Component
public class SessionManager implements DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    private final MiniSqlProperties properties;
    private final Map<String, ManagedSession> sessions = new ConcurrentHashMap<>();

    public SessionManager(MiniSqlProperties properties) {
        this.properties = properties;
    }

    /**
     * 创建一个新的 session，并返回 sessionId。
     */
    public String createSession() throws IOException {
        MiniSqlSession session = new MiniSqlSessionImpl(properties.getHost(), properties.getPort());
        String sessionId = UUID.randomUUID().toString();
        sessions.put(sessionId, new ManagedSession(session));
        return sessionId;
    }

    /**
     * 根据 sessionId 获取会话，不存在时抛出异常。
     */
    public MiniSqlSession getRequiredSession(String sessionId) {
        ManagedSession managed = sessions.get(sessionId);
        if (managed == null) {
            throw new SessionNotFoundException(sessionId);
        }
        managed.touch();
        return managed.session;
    }

    /**
     * 关闭并移除指定 session。
     *
     * @return true 表示存在且已关闭，false 表示 sessionId 不存在
     */
    public boolean closeSession(String sessionId) {
        ManagedSession managed = sessions.remove(sessionId);
        if (managed == null) {
            return false;
        }
        closeQuietly(sessionId, managed.session);
        return true;
    }

    @Override
    public void destroy() {
        sessions.forEach(this::closeQuietly);
        sessions.clear();
    }

    private void closeQuietly(String sessionId, ManagedSession managed) {
        closeQuietly(sessionId, managed.session);
    }

    private void closeQuietly(String sessionId, MiniSqlSession session) {
        try {
            session.close();
        } catch (Exception ex) {
            LOGGER.warn("关闭 session {} 失败", sessionId, ex);
        }
    }

    private static final class ManagedSession {
        private final MiniSqlSession session;
        private final Instant createdAt = Instant.now();
        private final AtomicLong lastAccessTs = new AtomicLong(createdAt.toEpochMilli());

        private ManagedSession(MiniSqlSession session) {
            this.session = session;
        }

        private void touch() {
            lastAccessTs.set(System.currentTimeMillis());
        }
    }
}
