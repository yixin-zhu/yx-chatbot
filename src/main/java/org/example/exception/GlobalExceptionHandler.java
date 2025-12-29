package org.example.exception;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CustomException.class)
    public ResponseEntity<Map<String, Object>> handleCustomException(CustomException ex) {
        Map<String, Object> body = new HashMap<>();
        body.put("code", ex.getStatus().value());
        body.put("message", ex.getMessage());
        body.put("success", false);

        return new ResponseEntity<>(body, ex.getStatus());
    }

    // 过渡用的RuntimeException处理器
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> handleRuntimeException(RuntimeException ex) {
        Map<String, Object> body = new HashMap<>();
        body.put("code", 500);
        body.put("message", "运行时错误: " + ex.getMessage());
        body.put("success", false);

        return ResponseEntity.internalServerError().body(body);
    }

    // 顺便处理一下其他的异常，防止报出白页
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneralException(Exception ex) {
        Map<String, Object> body = new HashMap<>();
        body.put("code", 500);
        body.put("message", "服务器内部错误: " + ex.getMessage());
        body.put("success", false);

        return ResponseEntity.internalServerError().body(body);
    }
}