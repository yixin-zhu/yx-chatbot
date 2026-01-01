package org.example.controller;
import io.micrometer.common.util.StringUtils;
import org.example.exception.CustomException;
import org.example.handler.ChatHandler;
import org.example.handler.ChatWebSocketHandler;
import org.example.utils.LogUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Map;

@Component
@RestController
@RequestMapping("/api/v1/chat")
public class ChatController extends TextWebSocketHandler {

    private final ChatHandler chatHandler;

    public ChatController(ChatHandler chatHandler) {
        this.chatHandler = chatHandler;
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message){
        String userMessage = message.getPayload();
        String userId = session.getId(); // Use session ID as userId for simplicity
        try {
            chatHandler.processMessage(userId, userMessage, session);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取WebSocket停止指令Token
     */
    @GetMapping("/websocket-token")
    public ResponseEntity<?> getWebSocketToken() {
        // 这里假设 ChatHandler 维护这个 Token 逻辑
        String cmdToken = ChatWebSocketHandler.getInternalCmdToken();

        if (StringUtils.isBlank(cmdToken)) {
            return ResponseEntity.badRequest().body(Map.of("error", "无法获取WebSocket命令令牌"));
        }

        return ResponseEntity.ok(Map.of("cmdToken", cmdToken));
    }
}
