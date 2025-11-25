package com.project.login.model.dto.notespace;

import lombok.Data;

@Data
public class NoteSpaceCreateDTO {
    private String name;
    private Long userId;
    private String tag;
}
