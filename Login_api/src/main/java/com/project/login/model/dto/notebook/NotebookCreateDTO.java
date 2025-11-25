package com.project.login.model.dto.notebook;

import lombok.*;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotebookCreateDTO {

    private String name;

    private Long spaceId;

    private String tag;
}