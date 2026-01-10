package com.project.login.model.dataobject;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Builder; // 建议添加 Builder 以保持与其他 DO 风格一致

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserDO {

    private Long id;

    private String username;

    private String studentNumber;

    private String email;

    private String password_hash;

    @Builder.Default
    private boolean enabled = false;

    @Builder.Default
    private Instant createdAt = Instant.now();

    @Builder.Default
    private Instant updatedAt = Instant.now();

    private String avatarUrl;
}