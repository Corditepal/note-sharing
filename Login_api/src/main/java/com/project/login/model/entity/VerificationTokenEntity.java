package com.project.login.model.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "verification_tokens")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VerificationTokenEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 255)
    private String email; // 与邮箱关联

    @Column(nullable = false, length = 128)
    private String token;

    @Column(nullable = false, length = 30)
    private String type; // "REGISTER" 或 "RESET"

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Column(name = "created_at", updatable = false)
    private Instant createdAt = Instant.now();

    // 构造函数方便创建
    public VerificationTokenEntity(String email, String token, String type, Instant expiresAt) {
        this.email = email;
        this.token = token;
        this.type = type;
        this.expiresAt = expiresAt;
        this.createdAt = Instant.now();
    }
}
