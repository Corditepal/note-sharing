package com.project.login.model.dataobject;

import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookDO {

    private Long id;

    private String name;

    private Long spaceId;

    private Long tagId;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    private List<NoteDO> notes;
}