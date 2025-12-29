package org.example.service;
import io.minio.*;
import io.minio.http.Method;
import org.apache.commons.codec.digest.DigestUtils;
import org.example.DTO.ChunkUploadDTO;
import org.example.DTO.FileProcessingTask;
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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    @Autowired
    private FileProcessingService fileProcessingService;

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
        // 1. 参数校验（如果 chunkIndex 来自前端，建议由 Controller 校验，这里作为兜底）
        if (chunkIndex < 0) {
            throw new CustomException("无效的分片索引", HttpStatus.BAD_REQUEST);
        }
        String redisKey = "upload:" + userId + ":" + fileMd5;
        return Boolean.TRUE.equals(redisTemplate.opsForValue().getBit(redisKey, chunkIndex));
    }

    /**
     * 标记指定分片为已上传
     *
     * @param fileMd5 文件的 MD5 值
     * @param chunkIndex 分片索引
     * @param userId 用户ID
     */
    public void markChunkUploaded(String fileMd5, int chunkIndex, String userId) {
        if (chunkIndex < 0) {
            logger.error("无效的分片索引 => fileMd5: {}, chunkIndex: {}", fileMd5, chunkIndex);
            throw new IllegalArgumentException("chunkIndex must be non-negative");
        }
        String redisKey = "upload:" + userId + ":" + fileMd5;
        redisTemplate.opsForValue().setBit(redisKey, chunkIndex, true);

    }

    /**
     * 删除文件所有分片上传标记(通常在合并成功或彻底删除文件时调用)
     *
     * @param fileMd5 文件的 MD5 值
     * @param userId 用户ID
     */
    public void deleteFileMark(String fileMd5, String userId) {
        String redisKey = "upload:" + userId + ":" + fileMd5;
        redisTemplate.delete(redisKey);
    }

    /**
     * 获取已上传的分片列表
     * @return 包含已上传分片索引的列表
     */
    public List<Integer> getUploadedChunks(String fileMd5, String userId) {
        List<Integer> uploadedChunks = new ArrayList<>();
        int totalChunks = getTotalChunks(fileMd5, userId);

        if (totalChunks <= 0) {
            return Collections.emptyList();
        }

        // 优化：一次性获取所有分片状态
        String redisKey = "upload:" + userId + ":" + fileMd5;
        byte[] bitmapData = redisTemplate.execute((RedisCallback<byte[]>) connection -> {
            return connection.get(redisKey.getBytes());
        });

        if (bitmapData == null) {
            return Collections.emptyList();
        }

     return IntStream.range(0, totalChunks)
                .filter(i -> isBitSet(bitmapData, i))
                .boxed()
                .collect(Collectors.toList());

    }

    /**
     * 检查bitmap中指定位置是否为1
     *
     * @param data bitmap数据
     * @param index 位索引
     * @return 指定位置是否为1
     */
    private boolean isBitSet(byte[] data, int index) {
        int byteIndex = index / 8;
        if (byteIndex >= data.length) return false;

        // Redis Bitmap 存储位顺序：从左到右 (高位到低位)
        int bitPosition = 7 - (index % 8);
        return (data[byteIndex] & (1 << bitPosition)) != 0;
    }

    private static final int DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024;
    /**
     * 获取文件的总分片数
     *
     * @param fileMd5 文件的 MD5 值
     * @param userId 用户ID
     * @return 文件的总分片数
     */
    // 从数据库的file_upload表中获取文件总大小，再根据默认分片大小，计算总分片数
    public int getTotalChunks(String fileMd5, String userId) {
        // 1. 获取文件记录，如果不存在则直接返回 0（或抛出业务异常，取决于你的业务需求）
        return fileUploadRepository.findByFileMd5AndUserId(fileMd5, userId)
                .map(file -> {
                    long totalSize = file.getTotalSize();
                    return (int) Math.ceil((double) totalSize / DEFAULT_CHUNK_SIZE);
                })
                .orElse(0);
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
        ChunkInfo chunkInfo = new ChunkInfo();
        chunkInfo.setFileMd5(fileMd5);
        chunkInfo.setChunkIndex(chunkIndex);
        chunkInfo.setChunkMd5(chunkMd5);
        chunkInfo.setStoragePath(storagePath);

        chunkInfoRepository.save(chunkInfo);

    }

    // 合并文件外面包裹的一层，处理前后各种工作
    @Transactional
    public String processFileMerge(String fileMd5, String fileName, String userId) {
        // 1. 权限与记录检查
        FileUpload fileUpload = fileUploadRepository.findByFileMd5AndUserId(fileMd5, userId)
                .orElseThrow(() -> new CustomException("文件记录不存在或无权操作", HttpStatus.NOT_FOUND));

        // 2. 完整性检查
        int uploadedCount = getUploadedChunks(fileMd5, userId).size();
        int totalCount = getTotalChunks(fileMd5, userId);
        if (uploadedCount < totalCount) {
            throw new CustomException("分片未全部上传 (" + uploadedCount + "/" + totalCount + ")", HttpStatus.BAD_REQUEST);
        }

        // 3. 执行物理合并
        String objectUrl = this.mergeChunks(fileMd5, fileName, userId);

        // 4. 重要：触发异步后续处理（向量化等）
        FileProcessingTask task = new FileProcessingTask(
                fileMd5, objectUrl, fileName,
                fileUpload.getUserId(), fileUpload.getOrgTag(), fileUpload.isPublic()
        );
        fileProcessingService.processTask(task);

        return objectUrl;
    }

    /**
     * 合并所有分片
     * @return 合成文件的访问 URL
     */
    public String mergeChunks(String fileMd5, String fileName, String userId) {
        // 1.获得文件所有的分片(分片完整性由外层保障)
        String fileType = getFileType(fileName);
        List<ChunkInfo> chunks = chunkInfoRepository.findByFileMd5OrderByChunkIndexAsc(fileMd5);


        // 2. 收集原始分片路径，准备一个合并路径
        String mergedPath = "merged/" + fileName;
        List<String> partPaths = chunks.stream().map(ChunkInfo::getStoragePath).collect(Collectors.toList());

        try {
            // 合并 (MinIO 操作)
            List<ComposeSource> sources = partPaths.stream()
                    .map(path -> ComposeSource.builder().bucket("uploads").object(path).build())
                    .collect(Collectors.toList());
            minioClient.composeObject(
                    ComposeObjectArgs.builder()
                            .bucket("uploads")
                            .object(mergedPath)
                            .sources(sources)
                            .build()
            );
            // 一系列后续业务处理 (清理 + 数据库更新)
            finalizeFileUpload(fileMd5, userId, mergedPath);
            // 返回预签名链接
            return minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket("uploads")
                            .object(mergedPath)
                            .expiry(168, TimeUnit.HOURS)
                            .build()
            );

        } catch (Exception e) {
            // 通过全局异常处理器处理
            throw new CustomException("文件合并失败: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void finalizeFileUpload(String fileMd5, String userId, String mergedPath) {
        // 1. 更新数据库FileUpload表里的文件状态
        FileUpload fileUpload = fileUploadRepository.findByFileMd5AndUserId(fileMd5, userId)
                .orElseThrow(() -> new CustomException("未找到文件记录", HttpStatus.NOT_FOUND));
        fileUpload.setStatus(1); // 已完成
        fileUpload.setMergedAt(LocalDateTime.now());
        fileUploadRepository.save(fileUpload);

        // 2. 删除 Redis 的分片上传标记
        deleteFileMark(fileMd5, userId);

        // 3. 删除 MinIO里的分片文件

        List<ChunkInfo> chunks = chunkInfoRepository.findByFileMd5(fileMd5);
        for (ChunkInfo chunk : chunks) {
            try {
                minioClient.removeObject(
                        RemoveObjectArgs.builder()
                                .bucket("uploads")
                                .object(chunk.getStoragePath())
                                .build()
                );
            } catch (Exception e) {
                throw new CustomException("删除分片文件失败: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

        // 4. 删除数据库中的分片记录（可选，取决于业务是否需要留底）
        chunkInfoRepository.deleteByFileMd5(fileMd5);
    }

}
