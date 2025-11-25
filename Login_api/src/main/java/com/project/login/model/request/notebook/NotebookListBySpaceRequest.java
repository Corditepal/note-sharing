package com.project.login.model.request.notebook;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 请求 DTO：根据笔记空间获取笔记本列表
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookListBySpaceRequest {

    @NotNull(message = "笔记空间ID不能为空")
    private Long spaceId;
}
