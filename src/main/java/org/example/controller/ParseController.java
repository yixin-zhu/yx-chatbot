package org.example.controller;
import org.example.annotation.LogAction;
import org.example.exception.CustomException;
import org.example.service.ParseService;
import org.example.utils.LogUtils;
import org.example.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;

@RestController
@RequestMapping("/api/v1/parse")
public class ParseController {

    @Autowired
    private ParseService parseService;

    // 文档解析接口
    // MultipartFile参数用于接收前端上传的文件
    @PostMapping
    @LogAction(value = "ParseController", action = "parseDocument")
    public ResponseEntity<String> parseDocument(@RequestParam("file") MultipartFile file,
                                                @RequestParam("file_md5") String fileMd5,
                                                @RequestAttribute(value = "userId", required = false) String userId) {
        try (InputStream is = file.getInputStream()) {
            parseService.parseAndSave(fileMd5, is, userId, "DEFAULT", false);
            return ResponseEntity.ok("文档解析成功");
        } catch (IOException e) {
            throw new CustomException("获取文件流失败: " + e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("文档解析失败：" + e.getMessage());
        }
    }
}