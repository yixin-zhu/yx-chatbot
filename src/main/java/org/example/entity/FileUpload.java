package org.example.entity;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

// 文件上传实体类，对应数据库中的 'file_upload' 表
@Data
@Entity
@Table(name = "file_upload")
public class FileUpload {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "file_md5", length = 32, nullable = false)
    private String fileMd5;

    private String fileName;

    // 以字节为单位的文件总大小
    private long totalSize;

    // 文件上传状态，0表示上传中，1表示已完成
    private int status;

    @Column(name = "user_id", length = 64, nullable = false)
    private String userId;


    @Column(name = "org_tag")
    private String orgTag;

    // true表示所有用户可访问，false表示仅组织内用户可访问
    @Column(name = "is_public", nullable = false)
    private boolean isPublic = false;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime mergedAt;
}
