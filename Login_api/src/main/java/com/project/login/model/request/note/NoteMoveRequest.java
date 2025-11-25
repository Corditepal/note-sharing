package com.project.login.model.request.note;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class NoteMoveRequest {

    @NotNull
    private Long noteId;

    @NotNull
    private Long targetNotebookId;
}
