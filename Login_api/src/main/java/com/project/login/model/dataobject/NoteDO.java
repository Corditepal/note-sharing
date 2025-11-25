package com.project.login.model.dataobject;

import lombok.*;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NoteDO {

    private Long id;

    private String title;

    private String filename;

    private String fileType;

    private Long notebookId;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}