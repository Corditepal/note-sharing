package com.project.login.model.request.notebook;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookUpdateRequest {

    @NotNull(message = "笔记本ID不能为空")
    private Long id;

    @NotBlank(message = "笔记本名称不能为空")
    private String name;

    @NotBlank(message = "标签不能为空")
    private String tag;
}
