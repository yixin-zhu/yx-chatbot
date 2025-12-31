package org.example.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.example.exception.CustomException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import org.springframework.http.HttpStatusCode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 嵌入向量生成客户端
@Component
public class EmbeddingClient {

    @Value("${embedding.api.model}")
    private String modelId;

    @Value("${embedding.api.batch-size:100}")
    private int batchSize;

    @Value("${embedding.api.dimension:2048}")
    private int dimension;

    private static final Logger logger = LoggerFactory.getLogger(EmbeddingClient.class);

    @Autowired
    private final WebClient webClient;

    private final ObjectMapper objectMapper;

    public EmbeddingClient(WebClient embeddingWebClient, ObjectMapper objectMapper) {
        this.webClient = embeddingWebClient;
        this.objectMapper = objectMapper;
    }

    /**
     * 调用 API 生成向量
     * @param texts 输入文本列表
     * @return 对应的向量列表
     */
    public List<float[]> embed(List<String> texts) {
        logger.info("开始生成向量，文本数量: {}", texts.size());
        List<float[]> allVectors = new ArrayList<>(texts.size());
        try {
            // 使用分批处理，避免请求体过大
            for (int i = 0; i < texts.size(); i += batchSize) {
                List<String> batch = texts.subList(i, Math.min(i + batchSize, texts.size()));
                // allVectors.addAll(processBatch(batch));
                String response = callApiOnce(batch);
                allVectors.addAll(parseVectors(response));
            }
            return allVectors;
        } catch (Exception e) {
            throw new CustomException("向量服务调用失败: " + e.getMessage(), HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private List<float[]> processBatch(List<String> batch) {
        Map<String, Object> body = Map.of(
                "model", modelId,
                "input", batch,
                "dimension", dimension,
                "encoding_format", "float"
        );

        // 1. 直接序列化为 DTO 对象，不再手动解析 String
        EmbeddingResponse response = webClient.post()
                .uri("/embeddings")
                .bodyValue(body)
                .retrieve()
                .onStatus(HttpStatusCode::isError, res -> Mono.error(new CustomException("API返回错误状态码", (HttpStatus) res.statusCode())))
                .bodyToMono(EmbeddingResponse.class) // 直接转 DTO
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)) // 指数退避算法更合理
                        .filter(e -> e instanceof WebClientResponseException))
                .block(Duration.ofSeconds(60));

        if (response == null || response.getData() == null) {
            throw new CustomException("API 响应为空", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        List<float[]> result = response.getData().stream()
                .map(EmbeddingResponse.DataNode::getEmbedding)
                .toList();

        if (!result.isEmpty()) {
            logger.info("生成的向量维度: {}, 第一个值: {}", result.get(0).length, result.get(0)[0]);
        }
        return result;
    }

    // --- 内部 DTO，利用 Jackson 自动映射 ---
    @Data
    public static class EmbeddingResponse {
        private List<DataNode> data;

        @Data
        public static class DataNode {
            private float[] embedding;
        }
    }


    private String callApiOnce(List<String> batch) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", modelId);
        requestBody.put("input", batch);
        requestBody.put("dimension", dimension);  // 直接在根级别设置dimension
        requestBody.put("encoding_format", "float");  // 添加编码格式

        return webClient.post()
                .uri("/embeddings")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(String.class)
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))
                        .filter(e -> e instanceof WebClientResponseException))
                .block(Duration.ofSeconds(30));
    }

    private List<float[]> parseVectors(String response) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(response);
        JsonNode data = jsonNode.get("data");  // 兼容模式下使用data字段
        if (data == null || !data.isArray()) {
            throw new RuntimeException("API 响应格式错误: data 字段不存在或不是数组");
        }

        List<float[]> vectors = new ArrayList<>();
        for (JsonNode item : data) {
            JsonNode embedding = item.get("embedding");
            if (embedding != null && embedding.isArray()) {
                float[] vector = new float[embedding.size()];
                for (int i = 0; i < embedding.size(); i++) {
                    vector[i] = (float) embedding.get(i).asDouble();
                }
                vectors.add(vector);
            }
        }
        return vectors;
    }
}
