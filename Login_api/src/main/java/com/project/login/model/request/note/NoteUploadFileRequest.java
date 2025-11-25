package com.project.login.model.request.note;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

@Data
public class NoteUploadFileRequest {

    @NotNull(message = "meta cannot be null")
    private NoteMeta meta;

    @NotNull(message = "file cannot be null")
    private MultipartFile file;
}
