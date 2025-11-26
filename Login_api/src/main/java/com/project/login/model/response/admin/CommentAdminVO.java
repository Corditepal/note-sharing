package com.project.login.model.response.admin;

import com.project.login.model.entity.CommentEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommentAdminVO {
    private Long id;
    private String content;
    private String username;
    private String note;
    private Long parentId;
    private Integer likes;
    private Integer replies;
    private String createdAt;

    public static CommentAdminVO from(CommentEntity c) {
        String username = c.getUser().getUsername();
        String noteTitle = c.getNote().getTitle();
        Long pid = c.getParent() == null ? null : c.getParent().getId();
        return new CommentAdminVO(
                c.getId(),
                c.getContent(),
                username,
                noteTitle,
                pid,
                c.getLikes(),
                c.getReplies(),
                c.getCreatedAt().toLocalDate().toString()
        );
    }
}