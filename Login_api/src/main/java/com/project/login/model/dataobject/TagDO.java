package com.project.login.model.dataobject;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TagDO {
    private Long id;
    private String name;
}