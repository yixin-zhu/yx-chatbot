package org.example.repository;
import org.example.entity.ChunkInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface ChunkInfoRepository extends JpaRepository<ChunkInfo, Long> {
    List<ChunkInfo> findByFileMd5OrderByChunkIndexAsc(String fileMd5);

    @Modifying
    @Transactional
    void deleteByFileMd5(String fileMd5);

    // 必须显式声明，Spring 才能在编译时识别它
    List<ChunkInfo> findByFileMd5(String fileMd5);
}
