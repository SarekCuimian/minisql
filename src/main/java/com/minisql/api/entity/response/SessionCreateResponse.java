package com.minisql.api.entity.response;

public class SessionCreateResponse {

    private final String sessionId;
    private final long createdAt;

    public SessionCreateResponse(String sessionId, long createdAt) {
        this.sessionId = sessionId;
        this.createdAt = createdAt;
    }

    public String getSessionId() {
        return sessionId;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
