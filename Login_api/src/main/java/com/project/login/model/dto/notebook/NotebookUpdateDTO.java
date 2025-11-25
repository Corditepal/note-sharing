package com.project.login.model.dto.notebook;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookUpdateDTO {

    private Long id;

    private String name;

    private String tag;
}