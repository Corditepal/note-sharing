package com.project.login.model.request.notebook;

import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookMoveRequest {

    @NotNull(message = "目标笔记空间ID不能为空")
    private Long targetNoteSpaceId;

    @NotNull(message = "笔记ID不能为空")
    private Long notebookId;
}
