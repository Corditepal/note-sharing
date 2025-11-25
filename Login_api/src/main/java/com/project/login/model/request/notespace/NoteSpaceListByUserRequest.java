package com.project.login.model.request.notespace;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class NoteSpaceListByUserRequest {
    @NotNull(message = "User ID cannot be null")
    private Long userId;
}
