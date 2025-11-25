package com.project.login.model.request.note;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NoteUpdateMeta {
    private Long id;
    private String title;
    private Long notebookId;
    private String fileType;
}
