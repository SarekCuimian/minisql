package com.minisql.api.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.minisql.api.entity.response.SessionCreateResponse;
import com.minisql.api.entity.request.SqlExecRequest;
import com.minisql.api.entity.response.SqlExecResponse;
import com.minisql.api.service.SqlService;
import com.minisql.api.exception.SessionNotFoundException;
import com.minisql.api.session.SessionManager;

import javax.validation.Valid;
import java.io.IOException;

@RestController
@RequestMapping("/api/sessions")
@Validated
public class SessionController {

    private final SessionManager sessionManager;
    private final SqlService sqlService;

    public SessionController(SessionManager sessionManager, SqlService sqlService) {
        this.sessionManager = sessionManager;
        this.sqlService = sqlService;
    }

    @PostMapping
    public ResponseEntity<SessionCreateResponse> createSession() {
        try {
            String sessionId = sessionManager.createSession();
            return ResponseEntity.ok(new SessionCreateResponse(sessionId, System.currentTimeMillis()));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
    }

    @PostMapping("/{sessionId}/sql")
    public ResponseEntity<SqlExecResponse<?>> executeWithSession(@PathVariable String sessionId,
                                                                 @Valid @RequestBody SqlExecRequest request) {
        try {
            // boolean text = request.getFormat() == null || request.getFormat().isText();
            SqlExecResponse<?> response = sqlService.executeWithSession(sessionId, request.getSql(), request.getFormat());
            return ResponseEntity.ok(response);
        } catch (SessionNotFoundException ex) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(SqlExecResponse.failure(ex.getMessage()));
        }
    }

    @DeleteMapping("/{sessionId}")
    public ResponseEntity<Void> closeSession(@PathVariable String sessionId) {
        boolean removed = sessionManager.closeSession(sessionId);
        if (removed) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
    }
}
