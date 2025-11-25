package com.project.login.model.request.note;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class NoteDeleteRequest {

    @NotNull
    private Long noteId;
}
