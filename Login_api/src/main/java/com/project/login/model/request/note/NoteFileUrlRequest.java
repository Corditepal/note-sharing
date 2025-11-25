package com.project.login.model.request.note;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class NoteFileUrlRequest {

    @NotBlank(message = "filename cannot be empty")
    private String filename; // MinIO 保存的对象名（objectName）
}
