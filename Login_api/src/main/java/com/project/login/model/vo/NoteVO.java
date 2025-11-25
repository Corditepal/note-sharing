package com.project.login.model.vo;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class NoteVO {

    private Long id;
    private String title;
    private String filename;
    private String fileType;

    private Long notebookId;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

}
