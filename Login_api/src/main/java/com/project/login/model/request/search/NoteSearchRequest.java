package com.project.login.model.request.search;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class NoteSearchRequest {
    @NotBlank
    private String keyword;
}
