package top.guoziyang.mydb.web.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import top.guoziyang.mydb.web.dto.SessionCreateResponse;
import top.guoziyang.mydb.web.dto.SqlExecRequest;
import top.guoziyang.mydb.web.dto.SqlExecResponse;
import top.guoziyang.mydb.web.service.SqlService;
import top.guoziyang.mydb.web.exception.SessionNotFoundException;
import top.guoziyang.mydb.web.session.SessionManager;

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
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "创建 session 失败: " + e.getMessage(), e);
        }
    }

    @PostMapping("/{sessionId}/sql")
    public ResponseEntity<SqlExecResponse> executeWithSession(@PathVariable String sessionId,
                                                              @Valid @RequestBody SqlExecRequest request) {
        try {
            SqlExecResponse response = sqlService.executeWithSession(sessionId, request.getSql());
            return ResponseEntity.ok(response);
        } catch (SessionNotFoundException ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        }
    }

    @DeleteMapping("/{sessionId}")
    public ResponseEntity<Void> closeSession(@PathVariable String sessionId) {
        boolean removed = sessionManager.closeSession(sessionId);
        if (removed) {
            return ResponseEntity.noContent().build();
        }
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Session 不存在: " + sessionId);
    }
}
