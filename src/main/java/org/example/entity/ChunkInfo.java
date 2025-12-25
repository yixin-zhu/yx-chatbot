package org.example.entity;

import jakarta.persistence.*;
import lombok.Data;

/**
 * 实体类，与数据库中的 'chunk_info' 表对应
 */
// 分片表，实现了大文件分块上传和存储机制
@Data
@Entity
@Table(name = "chunk_info")
public class ChunkInfo {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 所属文件的MD5值，用于标识该分块属于哪个文件
    private String fileMd5;

    // 分块的索引，表示这是文件的第几个分块
    private int chunkIndex;

    // 分块的MD5值，用于校验分块数据的完整性
    private String chunkMd5;

    // 分块在minIO存储系统中的路径
    private String storagePath;
}

