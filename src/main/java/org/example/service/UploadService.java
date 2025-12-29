package org.example.service;
import io.minio.*;
import io.minio.http.Method;
import org.apache.commons.codec.digest.DigestUtils;
import org.example.DTO.ChunkUploadDTO;
import org.example.entity.ChunkInfo;
import org.example.entity.FileUpload;
import org.example.exception.CustomException;
import org.example.repository.ChunkInfoRepository;
import org.example.repository.FileUploadRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class UploadService {

    private static final Logger logger = LoggerFactory.getLogger(UploadService.class);

    // 用于缓存已上传分片的信息
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // 用于与 MinIO 服务器交互
    @Autowired
    private MinioClient minioClient;

    // 用于操作文件上传记录的 Repository
    @Autowired
    private FileUploadRepository fileUploadRepository;

    // 用于操作分片信息的 Repository
    @Autowired
    private ChunkInfoRepository chunkInfoRepository;

    @Autowired
    private String minioPublicUrl; // 注入 MinIO 的公共访问地址

    @Autowired
    private FileTypeValidationService fileTypeValidationService;

    @Autowired
    private UserService userService;

    // 核心的分片上传，这个函数是外面包裹的一层，处理各种检查逻辑
    public Map<String, Object> processChunkUpload(ChunkUploadDTO dto, MultipartFile file, String userId) {
        // 1. 文件类型校验（仅在第一个分片）
        if (dto.getChunkIndex() == 0) {
            var result = fileTypeValidationService.validateFileType(dto.getFileName());
            if (!result.isValid()) {
                throw new CustomException(result.getMessage(), HttpStatus.BAD_REQUEST);
            }
        }

        // 2. 补全组织标签（如果为空）
        if (StringUtils.isEmpty(dto.getOrgTag())) {
            dto.setOrgTag(userService.getUserPrimaryOrg(userId));
        }

        // 3. 执行核心上传记录
        this.uploadChunk(dto.getFileMd5(), dto.getChunkIndex(), dto.getTotalSize(),
                dto.getFileName(), file, dto.getOrgTag(), dto.isPublic(), userId);

        // 4. 获取进度信息
        List<Integer> uploadedChunks = this.getUploadedChunks(dto.getFileMd5(), userId);
        int totalChunks = this.getTotalChunks(dto.getFileMd5(), userId);
        double progress = (double) uploadedChunks.size() / totalChunks * 100;

        // 5. 组装返回数据
        Map<String, Object> stats = new HashMap<>();
        stats.put("uploaded", uploadedChunks);
        stats.put("progress", progress);
        return stats;
    }

    // 核心的分片上传方法
    public void uploadChunk(String fileMd5, int chunkIndex, long totalSize, String fileName,
                            MultipartFile file, String orgTag, boolean isPublic, String userId)  {
        // 获取文件类型信息
        String fileType = getFileType(fileName);
        String contentType = file.getContentType();

        fileUploadRepository.findByFileMd5AndUserId(fileMd5, userId)
                .orElseGet(() -> createNewFileUpload(fileMd5, fileName, totalSize, userId, orgTag, isPublic));

        // 检查分片是否在Redis中标记为已上传
        boolean chunkInRedis = isChunkUploaded(fileMd5, chunkIndex, userId);

        // 检查分片是否在数据库中存在记录
        List<ChunkInfo> chunkInfos = chunkInfoRepository.findByFileMd5OrderByChunkIndexAsc(fileMd5);
        boolean chunkInDb = chunkInfos.stream()
                .anyMatch(chunk -> chunk.getChunkIndex() == chunkIndex);
        // 如果分片已在于Redis和数据库中，则跳过上传，直接返回
        if (chunkInRedis && chunkInDb) return;

        // 上传的准备工作. 获得路径和分片的 MD5
        String storagePath = "chunks/" + fileMd5 + "/" + chunkIndex;
        byte[] fileBytes = null;
        try {
            fileBytes = file.getBytes();
        } catch (IOException e) {
            throw new RuntimeException("读取分片文件失败: " + e.getMessage(), e);
        }
        String chunkMd5 = DigestUtils.md5Hex(fileBytes);

        // 若分片未上传，执行上传流程
        if (!chunkInRedis) {
            uploadToMinio(storagePath, file); // 封装 MinIO 的操作
            markChunkUploaded(fileMd5, chunkIndex, userId);
        }

        // 补全数据库记录
        if (!chunkInDb) {
            saveChunkInfo(fileMd5, chunkIndex, chunkMd5, storagePath);
        }
    }

    // 辅助函数：帮助上传分片到 MinIO
    private void uploadToMinio(String path, MultipartFile file) {
        try {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket("uploads")
                    .object(path)
                    .stream(file.getInputStream(), file.getSize(), -1)
                    .contentType(file.getContentType())
                    .build());
        } catch (Exception e) {
            // 将 MinIO 复杂的受检异常转为业务异常
            throw new CustomException("上传分片到存储服务器失败: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // 辅助函数：帮助在数据库中创建新的文件上传记录
    private FileUpload createNewFileUpload(String md5, String name, long size, String uid, String tag, boolean isPub) {
        FileUpload fu = new FileUpload();
        fu.setFileMd5(md5);
        fu.setFileName(name);
        fu.setTotalSize(size);
        fu.setStatus(0);
        fu.setUserId(uid);
        fu.setOrgTag(tag);
        fu.setPublic(isPub);
        return fileUploadRepository.save(fu);
    }

    // 获取上传状态
    public Map<String, Object> getUploadStatus(String fileMd5, String userId) {
        // 1. 获取文件基础信息
        FileUpload fileUpload = fileUploadRepository.findByFileMd5(fileMd5).orElse(null);

        String fileName = fileUpload != null ? fileUpload.getFileName() : "unknown";
        String fileType = fileUpload != null ? getFileType(fileName) : "unknown";

        // 2. 获取上传进度
        List<Integer> uploadedChunks = getUploadedChunks(fileMd5, userId);
        int totalChunks = getTotalChunks(fileMd5, userId);
        double progress = totalChunks > 0 ? (double) uploadedChunks.size() / totalChunks * 100 : 0.0;

        // 3. 组装数据
        Map<String, Object> data = new HashMap<>();
        data.put("uploaded", uploadedChunks);
        data.put("progress", progress);
        data.put("fileName", fileName);
        data.put("fileType", fileType);

        return data;
    }

    /**
     * 根据文件名获取文件类型
     *
     * @param fileName 文件名
     * @return 文件类型
     */
    private String getFileType(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return "unknown";
        }
        int lastDotIndex = fileName.lastIndexOf('.');
        if (lastDotIndex == -1 || lastDotIndex == fileName.length() - 1) {
            return "unknown";
        }
        String extension = fileName.substring(lastDotIndex + 1).toLowerCase();
        // 根据文件扩展名返回文件类型
        switch (extension) {
            case "pdf":
                return "PDF文档";
            case "doc":
            case "docx":
                return "Word文档";
            case "xls":
            case "xlsx":
                return "Excel表格";
            case "ppt":
            case "pptx":
                return "PowerPoint演示文稿";
            case "txt":
                return "文本文件";
            case "md":
                return "Markdown文档";
            case "json":
                return "JSON文件";
            case "xml":
                return "XML文件";
            case "csv":
                return "CSV文件";
            case "html":
            case "htm":
                return "HTML文件";
            case "css":
                return "CSS文件";
            case "js":
                return "JavaScript文件";
            case "java":
                return "Java源码";
            case "py":
                return "Python源码";
            case "cpp":
            case "c":
                return "C/C++源码";
            case "sql":
                return "SQL文件";
            default:
                return extension.toUpperCase() + "文件";
        }
    }

    /**
     * 检查指定分片是否已上传（单个查询版本，性能较低）
     * 注意：对于批量查询建议使用 getUploadedChunks() 方法
     *
     * @param fileMd5 文件的 MD5 值
     * @param chunkIndex 分片索引
     * @param userId 用户ID
     * @return 分片是否已上传
     */
    public boolean isChunkUploaded(String fileMd5, int chunkIndex, String userId) {
        logger.debug("检查分片是否已上传 => fileMd5: {}, chunkIndex: {}, userId: {}", fileMd5, chunkIndex, userId);
        try {
            if (chunkIndex < 0) {
                logger.error("无效的分片索引 => fileMd5: {}, chunkIndex: {}", fileMd5, chunkIndex);
                throw new IllegalArgumentException("chunkIndex must be non-negative");
            }
            String redisKey = "upload:" + userId + ":" + fileMd5;
            boolean isUploaded = redisTemplate.opsForValue().getBit(redisKey, chunkIndex);
            logger.debug("分片上传状态 => fileMd5: {}, chunkIndex: {}, userId: {}, isUploaded: {}",
                    fileMd5, chunkIndex, userId, isUploaded);
            return isUploaded;
        } catch (Exception e) {
            logger.error("检查分片上传状态失败 => fileMd5: {}, chunkIndex: {}, userId: {}, 错误: {}",
                    fileMd5, chunkIndex, userId, e.getMessage(), e);
            return false; // 或者根据业务需求返回其他值
        }
    }

    /**
     * 标记指定分片为已上传
     *
     * @param fileMd5 文件的 MD5 值
     * @param chunkIndex 分片索引
     * @param userId 用户ID
     */
    public void markChunkUploaded(String fileMd5, int chunkIndex, String userId) {
        logger.debug("标记分片为已上传 => fileMd5: {}, chunkIndex: {}, userId: {}", fileMd5, chunkIndex, userId);
        try {
            if (chunkIndex < 0) {
                logger.error("无效的分片索引 => fileMd5: {}, chunkIndex: {}", fileMd5, chunkIndex);
                throw new IllegalArgumentException("chunkIndex must be non-negative");
            }
            String redisKey = "upload:" + userId + ":" + fileMd5;
            redisTemplate.opsForValue().setBit(redisKey, chunkIndex, true);
            logger.debug("分片已标记为已上传 => fileMd5: {}, chunkIndex: {}, userId: {}", fileMd5, chunkIndex, userId);
        } catch (Exception e) {
            logger.error("标记分片为已上传失败 => fileMd5: {}, chunkIndex: {}, userId: {}, 错误: {}",
                    fileMd5, chunkIndex, userId, e.getMessage(), e);
            throw new RuntimeException("Failed to mark chunk as uploaded", e);
        }
    }

    /**
     * 删除文件所有分片上传标记
     *
     * @param fileMd5 文件的 MD5 值
     * @param userId 用户ID
     */
    public void deleteFileMark(String fileMd5, String userId) {
        logger.debug("删除文件所有分片上传标记 => fileMd5: {}, userId: {}", fileMd5, userId);
        try {
            String redisKey = "upload:" + userId + ":" + fileMd5;
            redisTemplate.delete(redisKey);
            logger.info("文件分片上传标记已删除 => fileMd5: {}, userId: {}", fileMd5, userId);
        } catch (Exception e) {
            logger.error("删除文件分片上传标记失败 => fileMd5: {}, userId: {}, 错误: {}", fileMd5, userId, e.getMessage(), e);
            throw new RuntimeException("Failed to delete file mark", e);
        }
    }


    /**
     * 获取已上传的分片列表
     *
     * @param fileMd5 文件的 MD5 值
     * @param userId 用户ID
     * @return 包含已上传分片索引的列表
     */
    public List<Integer> getUploadedChunks(String fileMd5, String userId) {
        logger.info("获取已上传分片列表 => fileMd5: {}, userId: {}", fileMd5, userId);
        List<Integer> uploadedChunks = new ArrayList<>();
        try {
            int totalChunks = getTotalChunks(fileMd5, userId);
            logger.debug("文件总分片数 => fileMd5: {}, userId: {}, totalChunks: {}", fileMd5, userId, totalChunks);

            if (totalChunks == 0) {
                logger.warn("文件总分片数为0 => fileMd5: {}, userId: {}", fileMd5, userId);
                return uploadedChunks;
            }

            // 优化：一次性获取所有分片状态
            String redisKey = "upload:" + userId + ":" + fileMd5;
            byte[] bitmapData = redisTemplate.execute((RedisCallback<byte[]>) connection -> {
                return connection.get(redisKey.getBytes());
            });

            if (bitmapData == null) {
                logger.info("Redis中无分片状态记录 => fileMd5: {}, userId: {}", fileMd5, userId);
                return uploadedChunks;
            }

            // 解析bitmap，找出已上传的分片
            for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
                if (isBitSet(bitmapData, chunkIndex)) {
                    uploadedChunks.add(chunkIndex);
                }
            }

            logger.info("获取到已上传分片列表 => fileMd5: {}, userId: {}, 已上传数量: {}, 总分片数: {}, 优化方式: 一次性获取",
                    fileMd5, userId, uploadedChunks.size(), totalChunks);
            return uploadedChunks;
        } catch (Exception e) {
            logger.error("获取已上传分片列表失败 => fileMd5: {}, userId: {}, 错误: {}", fileMd5, userId, e.getMessage(), e);
            throw new RuntimeException("Failed to get uploaded chunks", e);
        }
    }

    /**
     * 检查bitmap中指定位置是否为1
     *
     * @param bitmapData bitmap数据
     * @param bitIndex 位索引
     * @return 指定位置是否为1
     */
    private boolean isBitSet(byte[] bitmapData, int bitIndex) {
        try {
            int byteIndex = bitIndex / 8;
            int bitPosition = 7 - (bitIndex % 8); // Redis bitmap的位顺序是从高位到低位

            if (byteIndex >= bitmapData.length) {
                return false; // 超出范围的位默认为0
            }

            return (bitmapData[byteIndex] & (1 << bitPosition)) != 0;
        } catch (Exception e) {
            logger.error("检查bitmap位状态失败 => bitIndex: {}, 错误: {}", bitIndex, e.getMessage(), e);
            return false;
        }
    }

    /**
     * 获取文件的总分片数
     *
     * @param fileMd5 文件的 MD5 值
     * @param userId 用户ID
     * @return 文件的总分片数
     */
    // 从数据库的file_upload表中获取文件总大小，再根据默认分片大小，计算总分片数
    public int getTotalChunks(String fileMd5, String userId) {
        logger.info("计算文件总分片数 => fileMd5: {}, userId: {}", fileMd5, userId);
        try {
            Optional<FileUpload> fileUpload = fileUploadRepository.findByFileMd5AndUserId(fileMd5, userId);

            if (fileUpload.isEmpty()) {
                logger.warn("文件记录不存在，无法计算分片数 => fileMd5: {}, userId: {}", fileMd5, userId);
                return 0;
            }

            long totalSize = fileUpload.get().getTotalSize();
            // 默认每个分片5MB
            int chunkSize = 5 * 1024 * 1024;
            int totalChunks = (int) Math.ceil((double) totalSize / chunkSize);

            logger.info("文件总分片数计算结果 => fileMd5: {}, userId: {}, totalSize: {}, chunkSize: {}, totalChunks: {}",
                    fileMd5, userId, totalSize, chunkSize, totalChunks);
            return totalChunks;
        } catch (Exception e) {
            logger.error("计算文件总分片数失败 => fileMd5: {}, userId: {}, 错误: {}", fileMd5, userId, e.getMessage(), e);
            throw new RuntimeException("Failed to calculate total chunks", e);
        }
    }

    /**
     * 保存分片信息到数据库
     *
     * @param fileMd5 文件的 MD5 值
     * @param chunkIndex 分片索引
     * @param chunkMd5 分片的 MD5 值
     * @param storagePath 分片的存储路径
     */
    private void saveChunkInfo(String fileMd5, int chunkIndex, String chunkMd5, String storagePath) {
        logger.debug("保存分片信息到数据库 => fileMd5: {}, chunkIndex: {}, chunkMd5: {}, storagePath: {}",
                fileMd5, chunkIndex, chunkMd5, storagePath);
        try {
            ChunkInfo chunkInfo = new ChunkInfo();
            chunkInfo.setFileMd5(fileMd5);
            chunkInfo.setChunkIndex(chunkIndex);
            chunkInfo.setChunkMd5(chunkMd5);
            chunkInfo.setStoragePath(storagePath);

            chunkInfoRepository.save(chunkInfo);
            logger.debug("分片信息已保存 => fileMd5: {}, chunkIndex: {}", fileMd5, chunkIndex);
        } catch (Exception e) {
            logger.error("保存分片信息失败 => fileMd5: {}, chunkIndex: {}, 错误: {}",
                    fileMd5, chunkIndex, e.getMessage(), e);
            throw new RuntimeException("Failed to save chunk info", e);
        }
    }

    /**
     * 合并所有分片
     *
     * @param fileMd5 文件的 MD5 值
     * @param fileName 文件名
     * @param userId 用户ID
     * @return 合成文件的访问 URL
     */
    public String mergeChunks(String fileMd5, String fileName, String userId) {
        String fileType = getFileType(fileName);
        logger.info("开始合并文件分片 => fileMd5: {}, fileName: {}, fileType: {}, userId: {}", fileMd5, fileName, fileType, userId);
        try {
            // 查询所有分片信息
            logger.debug("查询分片信息 => fileMd5: {}, fileName: {}", fileMd5, fileName);
            List<ChunkInfo> chunks = chunkInfoRepository.findByFileMd5OrderByChunkIndexAsc(fileMd5);
            logger.info("查询到分片信息 => fileMd5: {}, fileName: {}, fileType: {}, 分片数量: {}", fileMd5, fileName, fileType, chunks.size());

            // 检查分片数量是否与预期一致
            int expectedChunks = getTotalChunks(fileMd5, userId);
            if (chunks.size() != expectedChunks) {
                logger.error("分片数量不匹配 => fileMd5: {}, fileName: {}, fileType: {}, 期望: {}, 实际: {}",
                        fileMd5, fileName, fileType, expectedChunks, chunks.size());
                throw new RuntimeException(String.format(
                        "分片数量不匹配，期望: %d, 实际: %d", expectedChunks, chunks.size()));
            }

            List<String> partPaths = chunks.stream()
                    .map(ChunkInfo::getStoragePath)
                    .collect(Collectors.toList());
            logger.debug("分片路径列表 => fileMd5: {}, fileName: {}, 路径数量: {}", fileMd5, fileName, partPaths.size());

            // 检查每个分片是否存在
            logger.info("开始检查每个分片是否存在 => fileMd5: {}, fileName: {}, fileType: {}", fileMd5, fileName, fileType);
            for (int i = 0; i < partPaths.size(); i++) {
                String path = partPaths.get(i);
                try {
                    StatObjectResponse stat = minioClient.statObject(
                            StatObjectArgs.builder()
                                    .bucket("uploads")
                                    .object(path)
                                    .build()
                    );
                    logger.debug("分片存在 => fileName: {}, index: {}, path: {}, size: {}", fileName, i, path, stat.size());
                } catch (Exception e) {
                    logger.error("分片不存在或无法访问 => fileName: {}, index: {}, path: {}, 错误: {}",
                            fileName, i, path, e.getMessage(), e);
                    throw new RuntimeException("分片 " + i + " 不存在或无法访问: " + e.getMessage(), e);
                }
            }
            logger.info("分片检查完成，所有分片都存在 => fileMd5: {}, fileName: {}, fileType: {}", fileMd5, fileName, fileType);

            String mergedPath = "merged/" + fileName;
            logger.info("开始合并分片 => fileMd5: {}, fileName: {}, fileType: {}, 合并后路径: {}", fileMd5, fileName, fileType, mergedPath);

            try {
                // 合并分片
                List<ComposeSource> sources = partPaths.stream()
                        .map(path -> ComposeSource.builder().bucket("uploads").object(path).build())
                        .collect(Collectors.toList());

                logger.debug("构建合并请求 => fileMd5: {}, fileName: {}, targetPath: {}, sourcePaths: {}",
                        fileMd5, fileName, mergedPath, partPaths);

                minioClient.composeObject(
                        ComposeObjectArgs.builder()
                                .bucket("uploads")
                                .object(mergedPath)
                                .sources(sources)
                                .build()
                );
                logger.info("分片合并成功 => fileMd5: {}, fileName: {}, fileType: {}, mergedPath: {}", fileMd5, fileName, fileType, mergedPath);

                // 检查合并后的文件
                StatObjectResponse stat = minioClient.statObject(
                        StatObjectArgs.builder()
                                .bucket("uploads")
                                .object(mergedPath)
                                .build()
                );
                logger.info("合并文件信息 => fileMd5: {}, fileName: {}, fileType: {}, path: {}, size: {}", fileMd5, fileName, fileType, mergedPath, stat.size());

                // 清理分片文件
                logger.info("开始清理分片文件 => fileMd5: {}, fileName: {}, 分片数量: {}", fileMd5, fileName, partPaths.size());
                for (String path : partPaths) {
                    try {
                        minioClient.removeObject(
                                RemoveObjectArgs.builder()
                                        .bucket("uploads")
                                        .object(path)
                                        .build()
                        );
                        logger.debug("分片文件已删除 => fileName: {}, path: {}", fileName, path);
                    } catch (Exception e) {
                        // 记录错误但不中断流程
                        logger.warn("删除分片文件失败，将继续处理 => fileName: {}, path: {}, 错误: {}", fileName, path, e.getMessage());
                    }
                }
                logger.info("分片文件清理完成 => fileMd5: {}, fileName: {}, fileType: {}", fileMd5, fileName, fileType);

                // 删除 Redis 中的分片状态记录
                logger.info("删除Redis中的分片状态记录 => fileMd5: {}, fileName: {}, userId: {}", fileMd5, fileName, userId);
                deleteFileMark(fileMd5, userId);
                logger.info("分片状态记录已删除 => fileMd5: {}, fileName: {}, userId: {}", fileMd5, fileName, userId);

                // 更新文件状态
                logger.info("更新文件状态为已完成 => fileMd5: {}, fileName: {}, fileType: {}, userId: {}", fileMd5, fileName, fileType, userId);
                FileUpload fileUpload = fileUploadRepository.findByFileMd5AndUserId(fileMd5, userId)
                        .orElseThrow(() -> {
                            logger.error("更新文件状态失败，文件记录不存在 => fileMd5: {}, fileName: {}", fileMd5, fileName);
                            return new RuntimeException("文件记录不存在: " + fileMd5);
                        });
                fileUpload.setStatus(1); // 已完成
                fileUpload.setMergedAt(LocalDateTime.now());
                fileUploadRepository.save(fileUpload);
                logger.info("文件状态已更新为已完成 => fileMd5: {}, fileName: {}, fileType: {}", fileMd5, fileName, fileType);

                // 生成预签名 URL（有效期为 1 小时）
                logger.info("开始生成预签名URL => fileMd5: {}, fileName: {}, path: {}", fileMd5, fileName, mergedPath);
                String presignedUrl = minioClient.getPresignedObjectUrl(
                        GetPresignedObjectUrlArgs.builder()
                                .method(Method.GET)
                                .bucket("uploads")
                                .object(mergedPath)
                                .expiry(168, TimeUnit.HOURS) // 设置有效期为 168 小时
                                .build()
                );
                logger.info("预签名URL已生成 => fileMd5: {}, fileName: {}, fileType: {}, URL: {}", fileMd5, fileName, fileType, presignedUrl);

                return presignedUrl;
            } catch (Exception e) {
                logger.error("合并文件失败 => fileMd5: {}, fileName: {}, fileType: {}, 错误类型: {}, 错误信息: {}",
                        fileMd5, fileName, fileType, e.getClass().getName(), e.getMessage(), e);
                throw new RuntimeException("合并文件失败: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            logger.error("文件合并过程中发生错误 => fileMd5: {}, fileName: {}, fileType: {}, 错误类型: {}, 错误信息: {}",
                    fileMd5, fileName, fileType, e.getClass().getName(), e.getMessage(), e);
            throw new RuntimeException("文件合并失败: " + e.getMessage(), e);
        }
    }
}
