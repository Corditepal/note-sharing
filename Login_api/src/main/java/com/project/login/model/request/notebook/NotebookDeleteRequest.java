package com.project.login.model.request.notebook;

import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookDeleteRequest {

    @NotNull(message = "笔记本ID不能为空")
    private Long id;
}
