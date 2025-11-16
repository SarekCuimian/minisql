package com.minisql.web.exception;

public class SessionNotFoundException extends RuntimeException {

    public SessionNotFoundException(String sessionId) {
        super("Session 不存在或已失效: " + sessionId);
    }
}
