package org.example.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;

import org.example.DTO.EsDocument;
import org.example.DTO.SearchResult;
import org.example.client.EmbeddingClient;
import org.example.entity.FileUpload;
import org.example.entity.User;
import org.example.exception.CustomException;
import org.example.repository.FileUploadRepository;
import org.example.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 混合搜索服务，结合文本匹配和向量相似度搜索
 * 支持权限过滤，确保用户只能搜索其有权限访问的文档
 */
@Service
public class HybridSearchService {

    private static final Logger logger = LoggerFactory.getLogger(HybridSearchService.class);

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private EmbeddingClient embeddingClient;

    @Autowired
    private UserService userService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrgTagCacheService orgTagCacheService;

    @Autowired
    private FileUploadRepository fileUploadRepository;

    /**
     * 使用文本匹配和向量相似度进行混合搜索，支持权限过滤
     * 该方法确保用户只能搜索其有权限访问的文档（自己的文档、公开文档、所属组织的文档）
     *
     * @param query  查询字符串
     * @param userId 用户ID
     * @param topK   返回结果数量
     * @return 搜索结果列表
     */
    public List<SearchResult> searchWithPermission(String query, String userId, int topK) {
        try {
            // 1. 准备权限参数
            String userDbId = getUserDbId(userId);
            List<String> orgTags = getUserEffectiveOrgTags(userId);
            List<Float> queryVector = embedToVectorList(query);

            Query permissionFilter = buildPermissionQuery(userDbId, orgTags);

            // 2. 核心: 搜索逻辑
            SearchResponse<EsDocument> response = esClient.search(s -> s
                            .index("knowledge_base")
                            .size(topK)
                            // 1. 顶层 KNN：负责向量语义召回
                            .knn(kn -> kn
                                    .field("vector")
                                    .queryVector(queryVector)
                                    .k(topK)
                                    .numCandidates(100)
                                    .filter(permissionFilter) // KNN 内部过滤权限
                                    .boost(10.0f)
                            )
                            // 2. 顶层 Query：负责关键词匹配 + 权限硬过滤
                            .query(q -> q
                                    .bool(b -> b
                                            .must(m -> m.match(ma -> ma.field("textContent").query(query)))
                                            .filter(permissionFilter)
                                    )
                            )
                    , EsDocument.class);

            return processHits(response);
        } catch (Exception e) {
            logger.error("混合搜索失败，转为兜底方案", e);
            return textOnlySearchWithPermission(query, userId, getUserEffectiveOrgTags(userId), topK);
        }
    }

    /**
     * 构建统一的权限过滤查询
     */
    private Query buildPermissionQuery(String userId, List<String> orgTags) {
        return Query.of(q -> q.bool(b -> b
                .should(s -> s.term(t -> t.field("userId").value(userId)))    // 自己的
                .should(s -> s.term(t -> t.field("isPublic").value(true)))   // 公开的
                .should(s -> {                                               // 组织的
                    if (orgTags == null || orgTags.isEmpty()) return s.matchNone(m -> m);
                    return s.terms(t -> t
                            .field("orgTag")
                            .terms(v -> v.value(orgTags.stream().map(FieldValue::of).toList()))
                    );
                })
                .minimumShouldMatch("1")
        ));
    }

    /**
     * 处理 ES 搜索响应，转换为业务 SearchResult 列表
     */
    private List<SearchResult> processHits(SearchResponse<EsDocument> response) {
        // 1. 从 SearchResponse 中提取 Hit 对象列表
        List<SearchResult> results = response.hits().hits().stream()
                .map(hit -> {
                    EsDocument doc = hit.source();
                    // 此时 hit.score() 是 RRF 融合后的分数或 KNN 分数
                    double score = hit.score() != null ? hit.score() : 0.0;
                    if (doc == null) return null;
                    // 打印调试日志
                    logger.debug("处理命中结果 - ID: {}, 分数: {}, 内容预览: {}",
                            doc.getFileMd5(), score,
                            doc.getTextContent().substring(0, Math.min(30, doc.getTextContent().length())));

                    // 转换为 DTO
                    return new SearchResult(
                            doc.getFileMd5(),
                            doc.getChunkId(),
                            doc.getTextContent(),
                            score,
                            doc.getUserId(),
                            doc.getOrgTag(),
                            doc.isPublic()
                    );
                })
                .filter(Objects::nonNull) // 过滤掉可能的空对象
                .toList();

        logger.info("ES 原始命中数量: {}, 转换后结果数量: {}", response.hits().total().value(), results.size());

        // 2. 关键：补充文件名（关联数据库查询）
        attachFileNames(results);

        return results;
    }

    private void attachFileNames(List<SearchResult> results) {
        if (results == null || results.isEmpty()) {
            return;
        }
        try {
            // 收集所有唯一的 fileMd5
            Set<String> md5Set = results.stream()
                    .map(SearchResult::getFileMd5)
                    .collect(Collectors.toSet());
            List<FileUpload> uploads = fileUploadRepository.findByFileMd5In(new java.util.ArrayList<>(md5Set));
            Map<String, String> md5ToName = uploads.stream()
                    .collect(Collectors.toMap(FileUpload::getFileMd5, FileUpload::getFileName));
            // 填充文件名
            results.forEach(r -> r.setFileName(md5ToName.get(r.getFileMd5())));
        } catch (Exception e) {
            logger.error("补充文件名失败", e);
        }
    }

    /**
     * 仅使用文本匹配的带权限搜索方法
     */
    private List<SearchResult> textOnlySearchWithPermission(String query, String userId, List<String> userEffectiveTags, int topK) {
        String userDbId = getUserDbId(userId);
        List<String> orgTags = getUserEffectiveOrgTags(userId);

        logger.info("执行兜底纯文本搜索: query={}, userId={}", query, userId);

        try {
            SearchResponse<EsDocument> response = esClient.search(s -> s
                            .index("knowledge_base")
                            .query(q -> q.bool(b -> b
                                    // 1. 内容匹配：增加 fuzziness(AUTO) 提高对错别字的容忍度
                                    .must(m -> m.match(ma -> ma
                                            .field("textContent")
                                            .query(query)
                                            .fuzziness("AUTO")
                                            .operator(Operator.Or) // 兜底时通常希望结果多一些，故用 Or
                                    ))
                                    // 2. 权限过滤：直接复用我们之前写好的统一权限函数
                                    .filter(buildPermissionQuery(userDbId, orgTags))
                            ))
                            .size(topK)
                            .minScore(0.1d) // 适当调低阈值，确保在 Embedding 挂掉时能搜到尽可能相关的内容
                    , EsDocument.class);

            // 3. 结果处理：直接复用 processHits，包含日志、转换和 attachFileNames
            return processHits(response);

        } catch (Exception e) {
            logger.error("兜底纯文本搜索彻底失败", e);
            return Collections.emptyList();
        }
    }

    /**
     * 原始搜索方法，不包含权限过滤，保留向后兼容性
     */
    public List<SearchResult> search(String query, int topK) {
        try {
            logger.debug("开始混合检索，查询: {}, topK: {}", query, topK);
            logger.warn("使用了没有权限过滤的搜索方法，建议使用 searchWithPermission 方法");

            // 生成查询向量
            final List<Float> queryVector = embedToVectorList(query);

            // 如果向量生成失败，仅使用文本匹配
            if (queryVector == null) {
                logger.warn("向量生成失败，仅使用文本匹配进行搜索");
                return textOnlySearch(query, topK);
            }

            SearchResponse<EsDocument> response = esClient.search(s -> {
                s.index("knowledge_base");
                int recallK = topK * 30;
                s.knn(kn -> kn
                        .field("vector")
                        .queryVector(queryVector)
                        .k(recallK)
                        .numCandidates(recallK)
                );

                // 过滤仅保留包含关键词的文本
                s.query(q -> q.match(m -> m.field("textContent").query(query)));

                // rescore BM25
                s.rescore(r -> r
                        .windowSize(recallK)
                        .query(rq -> rq
                                .queryWeight(0.2d)
                                .rescoreQueryWeight(1.0d)
                                .query(rqq -> rqq.match(m -> m
                                        .field("textContent")
                                        .query(query)
                                        .operator(Operator.And)
                                ))
                        )
                );
                s.size(topK);
                return s;
            }, EsDocument.class);

            return response.hits().hits().stream()
                    .map(hit -> {
                        assert hit.source() != null;
                        return new SearchResult(
                                hit.source().getFileMd5(),
                                hit.source().getChunkId(),
                                hit.source().getTextContent(),
                                hit.score()
                        );
                    })
                    .toList();
        } catch (Exception e) {
            logger.error("搜索失败", e);
            // 发生异常时尝试使用纯文本搜索作为后备方案
            try {
                logger.info("尝试使用纯文本搜索作为后备方案");
                return textOnlySearch(query, topK);
            } catch (Exception fallbackError) {
                logger.error("后备搜索也失败", fallbackError);
                throw new RuntimeException("搜索完全失败", fallbackError);
            }
        }
    }

    /**
     * 仅使用文本匹配的搜索方法
     */
    private List<SearchResult> textOnlySearch(String query, int topK) throws Exception {
        SearchResponse<EsDocument> response = esClient.search(s -> s
                        .index("knowledge_base")
                        .query(q -> q
                                .match(m -> m
                                        .field("textContent")
                                        .query(query)
                                )
                        )
                        .size(topK),
                EsDocument.class
        );

        return response.hits().hits().stream()
                .map(hit -> {
                    assert hit.source() != null;
                    return new SearchResult(
                            hit.source().getFileMd5(),
                            hit.source().getChunkId(),
                            hit.source().getTextContent(),
                            hit.score()
                    );
                })
                .toList();
    }

    /**
     * 生成查询向量，返回 List<Float>，失败时返回 null
     */
    private List<Float> embedToVectorList(String text) {
        try {
            List<float[]> vecs = embeddingClient.embed(List.of(text));
            if (vecs == null || vecs.isEmpty()) {
                logger.warn("生成的向量为空");
                return null;
            }
            float[] raw = vecs.get(0);
            List<Float> list = new ArrayList<>(raw.length);
            for (float v : raw) {
                list.add(v);
            }
            return list;
        } catch (Exception e) {
            logger.error("生成向量失败", e);
            return null;
        }
    }

    /**
     * 获取用户的有效组织标签（包含层级关系）
     */
    private List<String> getUserEffectiveOrgTags(String userId) {
        logger.debug("获取用户有效组织标签，用户ID: {}", userId);
        try {
            // 获取用户名
            User user;
            try {
                Long userIdLong = Long.parseLong(userId);
                user = userRepository.findById(userIdLong)
                        .orElseThrow(() -> new CustomException("User not found with ID: " + userId, HttpStatus.NOT_FOUND));
                logger.debug("通过ID找到用户: {}", user.getUsername());
            } catch (NumberFormatException e) {
                // 如果userId不是数字格式，则假设它就是username
                logger.debug("用户ID不是数字格式，作为用户名查找: {}", userId);
                user = userRepository.findByUsername(userId)
                        .orElseThrow(() -> new CustomException("User not found: " + userId, HttpStatus.NOT_FOUND));
                logger.debug("通过用户名找到用户: {}", user.getUsername());
            }

            // 通过orgTagCacheService获取用户的有效标签集合
            List<String> effectiveTags = orgTagCacheService.getUserEffectiveOrgTags(user.getUsername());
            logger.debug("用户 {} 的有效组织标签: {}", user.getUsername(), effectiveTags);
            return effectiveTags;
        } catch (Exception e) {
            logger.error("获取用户有效组织标签失败: {}", e.getMessage(), e);
            return Collections.emptyList(); // 返回空列表作为默认值
        }
    }

    /**
     * 获取用户的数据库ID用于权限过滤
     */
    private String getUserDbId(String userId) {
        logger.debug("获取用户数据库ID，用户ID: {}", userId);
        try {
            // 获取用户名
            User user;
            try {
                Long userIdLong = Long.parseLong(userId);
                logger.debug("解析用户ID为Long: {}", userIdLong);
                user = userRepository.findById(userIdLong)
                        .orElseThrow(() -> new CustomException("User not found with ID: " + userId, HttpStatus.NOT_FOUND));
                logger.debug("通过ID找到用户: {}", user.getUsername());
                return userIdLong.toString(); // 如果输入已经是数字ID，直接返回
            } catch (NumberFormatException e) {
                // 如果userId不是数字格式，则假设它就是username
                logger.debug("用户ID不是数字格式，作为用户名查找: {}", userId);
                user = userRepository.findByUsername(userId)
                        .orElseThrow(() -> new CustomException("User not found: " + userId, HttpStatus.NOT_FOUND));
                logger.debug("通过用户名找到用户: {}, ID: {}", user.getUsername(), user.getId());
                return user.getId().toString(); // 返回用户的数据库ID
            }
        } catch (Exception e) {
            logger.error("获取用户数据库ID失败: {}", e.getMessage(), e);
            throw new RuntimeException("获取用户数据库ID失败", e);
        }
    }


}
