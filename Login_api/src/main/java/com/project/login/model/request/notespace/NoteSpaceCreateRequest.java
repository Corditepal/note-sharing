package com.project.login.model.request.notespace;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class NoteSpaceCreateRequest {
    @NotBlank(message = "Space name cannot be blank")
    private String name;

    @NotNull(message = "User ID cannot be null")
    private Long userId;

    @NotBlank(message = "Tag cannot be blank")
    private String tag;
}

