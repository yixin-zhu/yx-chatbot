package org.example.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.function.Consumer;

@Service
public class ZhipuAIStreamClient {
    @Value("${api.key}")
    private String apiKey;

    private final String apiUrl = "https://open.bigmodel.cn/api/paas/v4/chat/completions";

    public ZhipuAIStreamClient() {

    }

    public ZhipuAIStreamClient(String apiKey) {
        // this.apiKey = apiKey;
    }

    /**
     * 流式调用ZhipuAI API
     */
    public void streamChatCompletion(String userMessage) throws Exception {
        try {
            // 构建请求体
            JSONObject requestBody = new JSONObject();
            requestBody.put("model", "glm-4.5-flash");

            JSONArray messages = new JSONArray();
            JSONObject message = new JSONObject();
            message.put("role", "user");
            message.put("content", userMessage);
            messages.put(message);

            requestBody.put("messages", messages);
            requestBody.put("temperature", 0.7);
            requestBody.put("max_tokens", 2048);
            requestBody.put("stream", false);

            // 发送请求并处理流式响应
            HttpResponse<String> response = Unirest.post(apiUrl)
                    .header("Authorization", "Bearer " + apiKey)
                    .header("Content-Type", "application/json")
                    .body(requestBody.toString())
                    .asString();

            if (response.getStatus() != 200) {
                throw new RuntimeException("API请求失败: " + response.getStatusText());
            }

            // 解析完整响应
            String json = response.getBody(); // 获取 JSON 字符串
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            // 读取 content 字段
            String content = root.path("choices")
                    .get(0)
                    .path("message")
                    .path("content")
                    .asText();

            System.out.println(content);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendResponseChunk(String chunk) {
        try {
            // 将chunk包装成JSON格式，匹配前端期望的数据结构
            Map<String, String> chunkResponse = Map.of("chunk", chunk);
            System.out.println(new JSONObject(chunkResponse).toString());
        } catch (Exception e) {
            System.err.println("处理响应块时出错: " + e.getMessage());
        }
    }
}