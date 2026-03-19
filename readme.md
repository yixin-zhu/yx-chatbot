# EnterpriseRAG: 企业私有知识检索与智能问答平台

EnterpriseRAG 是一个基于 RAG (Retrieval-Augmented Generation, 检索增强生成) 架构的智能问答后端系统。它允许用户上传文档，系统将自动解析、分块、向量化并存储到知识库中。在用户提问时，系统会从知识库中检索相关内容，并结合大语言模型 (LLM) 生成精准、专业的回答。

## 核心功能
- **文件分片上传**：支持大文件（如 PDF、DOCX）的分片上传，避免内存溢出。
- **文档管理与解析**：支持多种格式文档的上传与存储（基于 MinIO），并使用 Apache Tika 进行文本提取。
- **智能分块与向量化**：集成 HanLP 进行文本处理，调用 Embedding 模型（如智谱AI）将文本转换为向量。
- **混合检索**：基于 Elasticsearch 实现向量检索与全文检索的混合查询，提升检索准确率。
- **权限与安全**：基于 Spring Security 与 JWT 实现用户认证，支持细粒度的组织标签 (OrgTag) 权限控制。

## 技术栈

- **后端框架**：Spring Boot 3.4.2, Java 17
- **持久化层**：Spring Data JPA, MySQL
- **缓存与队列**：Redis (Token 缓存与状态管理), Kafka (异步任务处理)
- **向量数据库/搜索引擎**：Elasticsearch
- **对象存储**：MinIO
- **安全认证**：Spring Security, JWT
- **大模型接入**：DeepSeek (WebFlux WebClient), 智谱AI (zai-sdk)
- **文档处理**：Apache Tika, HanLP
- **网络通信**：WebSocket, HTTP/RESTful API

## 核心模块说明

- `controller/`：处理 API 请求（如文档上传、解析、检索、用户管理）及 WebSocket 会话连接。
- `service/`：包含核心业务逻辑，如文档处理流 (`FileProcessingService`)、向量化 (`VectorizationService`)、混合检索 (`HybridSearchService`)。
- `client/`：对接外部大语言模型 API（DeepSeek、ZhipuAI 等）。
- `handler/`：处理 WebSocket 聊天消息的具体逻辑。
- `config/`：配置 Elasticsearch、MinIO、Redis、Spring Security 及 WebSocket 等基础设施。

## 快速开始

### 1. 环境准备
确保您的本地或服务器已安装并运行以下服务：
- JDK 17+
- Maven 3.6+
- MySQL 8.0+
- Redis
- Elasticsearch 8.x
- MinIO
- Kafka (视业务配置需求)

### 2. 配置文件
在 `src/main/resources/` 目录下创建 `application.yml` 或 `application.properties`，并配置以下关键参数（当前代码库未包含）：
- 数据库连接信息 (MySQL, Redis, Elasticsearch)
- MinIO 访问凭证 (Endpoint, AccessKey, SecretKey)
- 大模型 API Keys (DeepSeek, 智谱AI)
- JWT 密钥

*注：Elasticsearch 索引的 mapping 配置文件可参考 `src/main/resources/es-mappings/knowledge_base.json`。*

### 3. 编译与运行
使用 Maven 编译并启动项目：
```bash
mvn clean install
mvn spring-boot:run
```
或者直接在 IDE 中运行 `XinChatbotApplication.java`。

## 接口说明
系统主要提供以下 REST API 接口：
- `/api/v1/user/*`：用户注册、登录与权限管理
- `/api/v1/documents/*`：文档的增删查改与访问控制
- `/api/v1/upload/*`：文件及分块上传
- `/api/v1/chat/*`：获取 WebSocket 令牌并建立聊天连接
- `/api/v1/search/*`：知识库独立检索测试

*(详细接口文档可结合 Swagger 或 Postman 进行查看与调试)*