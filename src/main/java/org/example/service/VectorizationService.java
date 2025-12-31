package org.example.service;
import org.example.DTO.EsDocument;
import org.example.DTO.TextChunk;
import org.example.client.EmbeddingClient;
import org.example.entity.DocumentVector;
import org.example.exception.CustomException;
import org.example.repository.DocumentVectorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

// 向量化服务类
@Service
public class VectorizationService {

    private static final Logger logger = LoggerFactory.getLogger(VectorizationService.class);

    @Autowired
    private EmbeddingClient embeddingClient;

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Autowired
    private DocumentVectorRepository documentVectorRepository;

    /**
     * 执行向量化操作
     * @param fileMd5 文件指纹
     * @param userId 上传用户ID
     * @param orgTag 组织标签
     * @param isPublic 是否公开
     */
    public void vectorize(String fileMd5, String userId, String orgTag, boolean isPublic) {
        // 1. 直接获取分块实体，避免中间转换 (fetchTextChunks 逻辑内聚)
        List<DocumentVector> chunks = documentVectorRepository.findByFileMd5(fileMd5);
        if (chunks.isEmpty()) {
            logger.warn("未找到可向量化的文本块 => fileMd5: {}", fileMd5);
            return;
        }

        try {
            // 2. 核心：提取文本并调用 Embedding 模型
            List<String> texts = chunks.stream().map(DocumentVector::getTextContent).toList();
            List<float[]> vectors = embeddingClient.embed(texts);

            // 3. 构建并批量索引 ES 文档
            List<EsDocument> esDocuments = IntStream.range(0, chunks.size())
                    .mapToObj(i -> buildEsDocument(chunks.get(i), vectors.get(i), fileMd5, userId, orgTag, isPublic))
                    .toList();

            elasticsearchService.bulkIndex(esDocuments);

            logger.info("向量化处理并入库成功 => fileMd5: {}, 总块数: {}", fileMd5, chunks.size());

        } catch (Exception e) {
            throw new CustomException("向量化过程异常: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    /**
     * 辅助方法：构建 ES 文档对象
     */
    private EsDocument buildEsDocument(DocumentVector chunk, float[] vector, String fileMd5,
                                       String userId, String orgTag, boolean isPublic) {
        return new EsDocument(
                UUID.randomUUID().toString(),
                fileMd5,
                chunk.getChunkId(),
                chunk.getTextContent(),
                vector,
                "deepseek-embed",
                userId,
                orgTag,
                isPublic
        );
    }

    /**
     * 获取文件分块内容
     * @param fileMd5 文件指纹
     * @return 分块内容列表
     */
    // 从数据库获取分块内容
    private List<TextChunk> fetchTextChunks(String fileMd5) {
        // 调用 Repository 查询数据
        List<DocumentVector> vectors = documentVectorRepository.findByFileMd5(fileMd5);

        // 转换为 TextChunk 列表
        return vectors.stream()
                .map(vector -> new TextChunk(
                        vector.getChunkId(),
                        vector.getTextContent()
                ))
                .toList();
    }
}