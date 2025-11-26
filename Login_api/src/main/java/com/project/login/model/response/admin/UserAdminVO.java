package com.project.login.model.response.admin;

import com.project.login.model.entity.UserEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserAdminVO {
    private Long id;
    private String username;
    private String studentId;
    private String email;
    private Instant createdAt;

    public static UserAdminVO from(UserEntity u) {
        return new UserAdminVO(u.getId(), u.getUsername(), u.getStudentNumber(), u.getEmail(), u.getCreatedAt());
    }
}