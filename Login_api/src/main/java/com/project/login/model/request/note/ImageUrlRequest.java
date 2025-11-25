package com.project.login.model.request.note;


import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

@Data
public class ImageUrlRequest {
    @NotNull(message = "file cannot be null")
    private MultipartFile file;
}
