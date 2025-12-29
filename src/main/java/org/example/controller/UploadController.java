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
import java.util.*;

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
    @LogAction(value = "UploadController", action = "mergeFile")
    public ResponseEntity<?> mergeFile(
            @RequestBody MergeRequest request,
            @RequestAttribute("userId") String userId) {

        String objectUrl = uploadService.processFileMerge(request.fileMd5(), request.fileName(), userId);
        Map<String, Object> data = Collections.singletonMap("object_url", objectUrl);
        return ResponseEntity.ok(Result.success("文件合并成功", data));

    }




    /**
     * 计算上传进度
     *
     * @param uploadedChunks 已上传的分片列表
     * @param totalChunks 总分片数量
     * @return 返回上传进度的百分比
     */
    private double calculateProgress(List<Integer> uploadedChunks, int totalChunks) {
        return totalChunks>0 ? (double) uploadedChunks.size() / totalChunks * 100 : 0.0;
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
    public ResponseEntity<?> getSupportedFileTypes() {
        // 1. 直接从 Service 获取数据
        Map<String, Object> data = Map.of(
                "supportedTypes", fileTypeValidationService.getSupportedFileTypes(),
                "supportedExtensions", fileTypeValidationService.getSupportedExtensions(),
                "description", "系统支持的文档类型文件，这些文件可以被解析并进行向量化处理"
        );

        // 2. 利用 Result.success 统一包装返回
        return ResponseEntity.ok(Result.success("获取支持的文件类型成功", data));
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

