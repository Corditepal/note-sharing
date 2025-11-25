package com.project.login.model.vo;

import lombok.Data;

@Data
public class NoteSpaceVO {

    private Long id;
    private String name;
    private Long userId;
    private Long tag;
    private String createTime;
    private String updateTime;
}
