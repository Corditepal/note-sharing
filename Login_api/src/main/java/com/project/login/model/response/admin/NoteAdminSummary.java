package com.project.login.model.response.admin;

import com.project.login.model.entity.NoteEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NoteAdminSummary {
    private Long id;
    private String author;
    private String space;
    private String notebook;
    private String tag;
    private String preview;

    public static NoteAdminSummary from(NoteEntity n) {
        String author = n.getNotebook().getSpace().getUser().getUsername();
        String space = n.getNotebook().getSpace().getName();
        String notebook = n.getNotebook().getName();
        String tag = n.getNotebook().getTag().getName();
        String preview = n.getTitle();
        return new NoteAdminSummary(n.getId(), author, space, notebook, tag, preview);
    }
}
