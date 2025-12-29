package org.example.DTO;

import lombok.Data;

@Data
public class ChunkUploadDTO {
    private String fileMd5;
    private int chunkIndex;
    private long totalSize;
    private String fileName;
    private Integer totalChunks;
    private String orgTag;
    private boolean isPublic;

    public boolean isPublic() {
        return isPublic;
    }
}