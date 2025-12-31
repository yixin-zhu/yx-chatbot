package org.example.service;
import io.minio.MinioClient;
import io.minio.errors.*;
import lombok.extern.slf4j.Slf4j;
import org.example.DTO.FileProcessingTask;
import org.example.exception.CustomException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Service
@Slf4j
public class FileProcessingService {

    private final ParseService parseService;
    private final VectorizationService vectorizationService;



    public FileProcessingService(ParseService parseService, VectorizationService vectorizationService) {
        this.parseService = parseService;
        this.vectorizationService = vectorizationService;
    }

    @Async
    @Transactional
    public void processTask(FileProcessingTask task) {
        log.info("开始处理文件解析与向量化任务 => fileMd5: {}, userId: {}", task.getFileMd5(), task.getUserId());

        // 使用 Try-with-resources 自动关闭 InputStream
        try (InputStream fileStream = downloadFileFromStorage(task.getFilePath())) {

            // 1. 校验流是否合法
            InputStream processedStream = wrapAsBuffered(fileStream);

            // 2. 核心：调用parse service 解析文件并保存
            parseService.parseAndSave(task.getFileMd5(), processedStream,
                    task.getUserId(), task.getOrgTag(), task.isPublic());

            // 3. 向量化处理
            vectorizationService.vectorize(task.getFileMd5(),
                    task.getUserId(), task.getOrgTag(), task.isPublic());

            log.info("文件后续处理全部完成 => fileMd5: {}", task.getFileMd5());

        } catch (IOException e) {
            throw new CustomException("文件流下载或处理异常: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            // 这里的 Exception 用来捕获解析和向量化过程中的业务异常
            log.error("处理任务失败: fileMd5={}", task.getFileMd5(), e);
            throw new CustomException("任务处理中断: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 内部辅助：将流包装为可标记/缓冲流
     */
    private InputStream wrapAsBuffered(InputStream inputStream) throws IOException {
        if (inputStream == null) {
            throw new IOException("无法从存储中获取文件流");
        }
        return inputStream.markSupported() ? inputStream : new BufferedInputStream(inputStream);
    }

    /**
     * 模拟从存储系统下载文件
     *
     * @param filePath 文件路径或 URL
     * @return 文件输入流
     */
    private InputStream downloadFileFromStorage(String filePath) throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        log.info("Downloading file from storage: {}", filePath);

        try {
            // 如果是文件系统路径
            File file = new File(filePath);
            if (file.exists()) {
                log.info("Detected file system path: {}", filePath);
                return new FileInputStream(file);
            }

            // 如果是远程 URL
            if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
                log.info("Detected remote URL: {}", filePath);
                URL url = new URL(filePath);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(30000); // 连接超时30秒
                connection.setReadTimeout(180000);   // 读取超时时间3分钟

                // 添加必要的请求头
                connection.setRequestProperty("User-Agent", "SmartPAI-FileProcessor/1.0");

                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    log.info("Successfully connected to URL, starting download...");
                    return connection.getInputStream();
                } else if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
                    log.error("Access forbidden - possible expired presigned URL");
                    throw new IOException("Access forbidden - the presigned URL may have expired");
                } else {
                    log.error("Failed to download file, HTTP response code: {} for URL: {}", responseCode, filePath);
                    throw new IOException(String.format("Failed to download file, HTTP response code: %d", responseCode));
                }
            }

            // 如果既不是文件路径也不是 URL
            throw new IllegalArgumentException("Unsupported file path format: " + filePath);
        } catch (Exception e) {
            log.error("Error downloading file from storage: {}", filePath, e);
            return null; // 或者抛出异常
        }
    }
}