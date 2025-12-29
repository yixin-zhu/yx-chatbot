package org.example.controller;
import org.example.DTO.ChunkUploadDTO;
import org.example.DTO.FileProcessingTask;
import org.example.annotation.LogAction;
import org.example.entity.FileUpload;
import org.example.repository.FileUploadRepository;
import org.example.service.FileProcessingService;
import org.example.service.FileTypeValidationService;
import org.example.service.UploadService;
import org.example.service.UserService;
import org.example.utils.LogUtils;
import org.example.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RestController
@RequestMapping("/api/v1/upload")
public class UploadController {

    @Autowired
    private UploadService uploadService;

    @Autowired
    private UserService userService;

    @Autowired
    private FileUploadRepository fileUploadRepository;

    @Autowired
    private FileTypeValidationService fileTypeValidationService;

    @Autowired
    private FileProcessingService fileProcessingService;

    /**
     * 上传文件分片接口
     */
    // 把文件上传到MinIO，然后记录分片信息到数据库
    // 分片上传接口
    @PostMapping("/chunk")
    @LogAction(value = "UploadController", action = "uploadChunk")
    public ResponseEntity<?> uploadChunk(
            @ModelAttribute ChunkUploadDTO dto,
            @RequestParam("file") MultipartFile file,
            @RequestAttribute("userId") String userId) throws IOException {

        Map<String, Object> uploadStats = uploadService.processChunkUpload(dto, file, userId);
        return ResponseEntity.ok(Result.success("分片上传成功", uploadStats));
    }

    /**
     * 获取文件上传状态接口
     */
    // 查询上传状态接口
    @GetMapping("/status")
    @LogAction(value = "UploadController", action = "getUploadStatus")
    public ResponseEntity<?> getUploadStatus(@RequestParam("file_md5") String fileMd5,
                                                               @RequestAttribute("userId") String userId) {
        Map<String, Object> statusData = uploadService.getUploadStatus(fileMd5, userId);

        return ResponseEntity.ok(Result.success("获取上传状态成功", statusData));
    }

    /**
     * 合并文件分片接口
     *
     * @param request 包含文件MD5和文件名的请求体
     * @param userId 当前用户ID
     * @return 返回包含合并后文件访问URL的响应
     */
    // 文件合并接口
    @Transactional
    @PostMapping("/merge")
    public ResponseEntity<Map<String, Object>> mergeFile(
            @RequestBody MergeRequest request,
            @RequestAttribute("userId") String userId) {

        LogUtils.PerformanceMonitor monitor = LogUtils.startPerformanceMonitor("MERGE_FILE");
        try {
            String fileType = getFileType(request.fileName());
            LogUtils.logBusiness("MERGE_FILE", userId, "接收到合并文件请求: fileMd5=%s, fileName=%s, fileType=%s",
                    request.fileMd5(), request.fileName(), fileType);

            // 检查文件完整性和权限
            LogUtils.logBusiness("MERGE_FILE", userId, "检查文件记录和权限: fileMd5=%s, fileName=%s", request.fileMd5(), request.fileName());
            FileUpload fileUpload = fileUploadRepository.findByFileMd5AndUserId(request.fileMd5(), userId)
                    .orElseThrow(() -> {
                        LogUtils.logUserOperation(userId, "MERGE_FILE", request.fileMd5(), "FAILED_FILE_NOT_FOUND");
                        return new RuntimeException("文件记录不存在");
                    });

            // 确保用户有权限操作该文件
            if (!fileUpload.getUserId().equals(userId)) {
                LogUtils.logUserOperation(userId, "MERGE_FILE", request.fileMd5(), "FAILED_PERMISSION_DENIED");
                LogUtils.logBusiness("MERGE_FILE", userId, "权限验证失败: 尝试合并不属于自己的文件, fileMd5=%s, fileName=%s, 实际所有者=%s",
                        request.fileMd5(), request.fileName(), fileUpload.getUserId());
                monitor.end("合并失败：权限不足");
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("code", HttpStatus.FORBIDDEN.value());
                errorResponse.put("message", "没有权限操作此文件");
                return ResponseEntity.status(HttpStatus.FORBIDDEN).body(errorResponse);
            }

            LogUtils.logBusiness("MERGE_FILE", userId, "权限验证通过，开始合并文件: fileMd5=%s, fileName=%s, fileType=%s", request.fileMd5(), request.fileName(), fileType);

            // 检查分片是否全部上传完成
            List<Integer> uploadedChunks = uploadService.getUploadedChunks(request.fileMd5(), userId);
            int totalChunks = uploadService.getTotalChunks(request.fileMd5(), userId);
            LogUtils.logBusiness("MERGE_FILE", userId, "分片上传状态: fileMd5=%s, fileName=%s, 已上传=%d/%d",
                    request.fileMd5(), request.fileName(), uploadedChunks.size(), totalChunks);

            if (uploadedChunks.size() < totalChunks) {
                LogUtils.logUserOperation(userId, "MERGE_FILE", request.fileMd5(), "FAILED_INCOMPLETE_CHUNKS");
                monitor.end("合并失败：分片未全部上传");
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("code", HttpStatus.BAD_REQUEST.value());
                errorResponse.put("message", "文件分片未全部上传，无法合并");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
            }

            // 合并文件
            LogUtils.logBusiness("MERGE_FILE", userId, "开始合并文件分片: fileMd5=%s, fileName=%s, fileType=%s, 分片数量=%d", request.fileMd5(), request.fileName(), fileType, totalChunks);
            String objectUrl = uploadService.mergeChunks(request.fileMd5(), request.fileName(), userId);
            LogUtils.logFileOperation(userId, "MERGE", request.fileName(), request.fileMd5(), "SUCCESS");

            // 处理文件提取和向量化
            FileProcessingTask task = new FileProcessingTask(
                    request.fileMd5(),
                    objectUrl,
                    request.fileName(),
                    fileUpload.getUserId(),
                    fileUpload.getOrgTag(),
                    fileUpload.isPublic()
            );

            fileProcessingService.processTask(task);

            // 构建数据对象
            Map<String, Object> data = new HashMap<>();
            data.put("object_url", objectUrl);

            // 构建统一响应格式
            Map<String, Object> response = new HashMap<>();
            response.put("code", 200);
            response.put("message", "文件合并成功");
            response.put("data", data);

            LogUtils.logUserOperation(userId, "MERGE_FILE", request.fileMd5(), "SUCCESS");
            monitor.end("文件合并成功");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            String fileType = getFileType(request.fileName());
            LogUtils.logBusinessError("MERGE_FILE", userId, "文件合并失败: fileMd5=%s, fileName=%s, fileType=%s", e,
                    request.fileMd5(), request.fileName(), fileType);
            monitor.end("文件合并失败: " + e.getMessage());
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("code", HttpStatus.INTERNAL_SERVER_ERROR.value());
            errorResponse.put("message", "文件合并失败: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * 计算上传进度
     *
     * @param uploadedChunks 已上传的分片列表
     * @param totalChunks 总分片数量
     * @return 返回上传进度的百分比
     */
    private double calculateProgress(List<Integer> uploadedChunks, int totalChunks) {
        if (totalChunks == 0) {
            LogUtils.logBusiness("CALCULATE_PROGRESS", "system", "计算上传进度时总分片数为0");
            return 0.0;
        }
        return (double) uploadedChunks.size() / totalChunks * 100;
    }

    /**
     * 合并请求的辅助类，包含文件的MD5值和文件名
     */
    public record MergeRequest(String fileMd5, String fileName) {}

    /**
     * 获取支持的文件类型列表接口
     *
     * @return 返回支持的文件类型信息
     */
    @GetMapping("/supported-types")
    public ResponseEntity<Map<String, Object>> getSupportedFileTypes() {
        LogUtils.PerformanceMonitor monitor = LogUtils.startPerformanceMonitor("GET_SUPPORTED_TYPES");
        try {
            LogUtils.logBusiness("GET_SUPPORTED_TYPES", "system", "获取支持的文件类型列表");

            Set<String> supportedTypes = fileTypeValidationService.getSupportedFileTypes();
            Set<String> supportedExtensions = fileTypeValidationService.getSupportedExtensions();

            // 构建数据对象
            Map<String, Object> data = new HashMap<>();
            data.put("supportedTypes", supportedTypes);
            data.put("supportedExtensions", supportedExtensions);
            data.put("description", "系统支持的文档类型文件，这些文件可以被解析并进行向量化处理");

            // 构建统一响应格式
            Map<String, Object> response = new HashMap<>();
            response.put("code", 200);
            response.put("message", "获取支持的文件类型成功");
            response.put("data", data);

            LogUtils.logBusiness("GET_SUPPORTED_TYPES", "system", "成功返回支持的文件类型: 类型数量=%d, 扩展名数量=%d",
                    supportedTypes.size(), supportedExtensions.size());
            monitor.end("获取支持的文件类型成功");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LogUtils.logBusinessError("GET_SUPPORTED_TYPES", "system", "获取支持的文件类型失败", e);
            monitor.end("获取支持的文件类型失败: " + e.getMessage());
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("code", HttpStatus.INTERNAL_SERVER_ERROR.value());
            errorResponse.put("message", "获取支持的文件类型失败: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
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
}

