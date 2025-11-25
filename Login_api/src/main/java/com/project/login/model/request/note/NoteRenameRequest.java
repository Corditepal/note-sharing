package com.project.login.model.request.note;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class NoteRenameRequest {

    @NotNull(message = "Note id cannot be null")
    private Long id;

    @NotBlank(message = "New name cannot be blank")
    private String newName;
}
