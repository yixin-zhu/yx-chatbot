package org.example.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;

// 聊天消息实体类
@Data
@AllArgsConstructor
public class Message {
    private String role;
    private String content;
}
