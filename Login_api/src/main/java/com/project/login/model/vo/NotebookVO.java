package com.project.login.model.vo;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class NotebookVO {
    private Long id;
    private String name;

    private Long spaceId;

    private Long tagId;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
