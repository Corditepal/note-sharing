package com.project.login.model.dto.notebook;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookMoveDTO {

    private Long targetNoteSpaceId;

    private Long notebookId;
}