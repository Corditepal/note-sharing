package com.project.login.model.request.notebook;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookCreateRequest {

    @NotBlank(message = "笔记本名称不能为空")
    private String name;

    @NotNull(message = "所属空间ID不能为空")
    private Long spaceId;

    @NotBlank(message = "标签不能为空")
    private String tag;
}
