package com.project.login.model.response.notespace;

import com.project.login.model.entity.NoteSpaceEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NoteSpaceVO {
    private Long id;
    private String name;
    private Long userId;
    private String tag;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static NoteSpaceVO from(NoteSpaceEntity e) {
        return new NoteSpaceVO(
                e.getId(),
                e.getName(),
                e.getUser() != null ? e.getUser().getId() : null,
                e.getTag() != null ? e.getTag().getName() : null,
                e.getCreatedAt(),
                e.getUpdatedAt()
        );
    }
}