package com.project.login.model.dataobject;

import lombok.*;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NoteSpaceDO {

    private Long id;          // 笔记空间 ID
    private String name;      // 笔记空间名称
    private Long userId;      // 用户 ID
    private Long tagId;       // 标签 ID
    private LocalDateTime createdAt;  // 创建时间
    private LocalDateTime updatedAt;  // 更新时间
}
