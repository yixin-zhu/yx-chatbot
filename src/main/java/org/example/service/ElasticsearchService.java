package org.example.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import org.example.DTO.EsDocument;
import org.example.exception.CustomException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

// Elasticsearch服务
@Service
public class ElasticsearchService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    @Autowired
    private ElasticsearchClient esClient;

    private static final String INDEX_NAME = "knowledge_base";

    @Value("${embedding.api.dimension:2048}") // 从配置中读取维度，必须与模型输出一致
    private int dimension;

    /**
     * 批量索引文档到Elasticsearch中
     * 通过接收一个EsDocument对象列表，将这些文档批量索引到名为"knowledge_base"的索引中
     * 使用Elasticsearch的Bulk API来执行批量索引操作，以提高索引效率
     *
     * @param documents 文档列表，每个文档都将被索引到Elasticsearch中
     */
    public void bulkIndex(List<EsDocument> documents) {
        logger.info("开始批量索引文档到Elasticsearch，文档数量: {}", documents.size());
        try {

            ensureIndexExists();
            // 将文档列表转换为批量操作列表，每个文档都对应一个索引操作
            List<BulkOperation> bulkOperations = documents.stream()
                    .map(doc -> BulkOperation.of(op -> op.index(idx -> idx
                            .index(INDEX_NAME) // 指定索引名称
                            .id(doc.getFileMd5() + "_" + doc.getChunkId()) // 使用文档的ID作为Elasticsearch中的文档ID
                            .document(doc) // 将文档对象作为数据源
                    )))
                    .toList();

            // 创建BulkRequest对象，并将批量操作列表添加到请求中
            BulkRequest request = BulkRequest.of(b -> b.operations(bulkOperations));

            // 执行批量索引操作
            BulkResponse response = esClient.bulk(request);

            // 检查响应结果
            if (response.errors()) {
                response.items().stream()
                        .filter(item -> item.error() != null)
                        .forEach(item -> logger.error("文档 {} 索引失败: {}", item.id(), item.error().reason()));
                throw new CustomException("批量索引部分失败，请检查日志", HttpStatus.INTERNAL_SERVER_ERROR);
            } else {
                logger.info("批量索引成功完成，文档数量: {}", documents.size());
            }
        } catch (Exception e) {
            logger.error("批量索引失败，文档数量: {}", documents.size(), e);
            throw new RuntimeException("批量索引失败", e);
        }
    }

    /**
     * 确保索引存在，并配置 dense_vector 映射
     */
    private void ensureIndexExists() throws IOException {
        BooleanResponse exists = esClient.indices().exists(e -> e.index(INDEX_NAME));

        if (!exists.value()) {
            logger.info("索引 {} 不存在，正在从 knowledge_base.json 加载配置创建索引...", INDEX_NAME);

            // 1. 从 resources 加载 JSON 文件
            ClassPathResource resource = new ClassPathResource("es-mappings/knowledge_base.json");
            try (InputStream is = resource.getInputStream()) {
                // 2. 将 InputStream 转换为 ES 能够理解的 JsonData 或直接作为请求体
                // 注意：Java Client 8.x 支持通过 Source 直接创建
                esClient.indices().create(c -> c
                        .index(INDEX_NAME)
                        .withJson(is) // 核心：直接加载 JSON 内容
                );
            }
            logger.info("索引 {} 创建成功", INDEX_NAME);
        }
    }

    /**
     * 根据file_md5删除文档
     * @param fileMd5 文件指纹
     */
    public void deleteByFileMd5(String fileMd5) {
        try {
            DeleteByQueryRequest request = DeleteByQueryRequest.of(d -> d
                    .index("knowledge_base")
                    .query(q -> q.term(t -> t.field("fileMd5").value(fileMd5)))
            );
            esClient.deleteByQuery(request);
        } catch (Exception e) {
            throw new RuntimeException("删除文档失败", e);
        }
    }
}