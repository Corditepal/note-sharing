CREATE TABLE IF NOT EXISTS users
(
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    username            VARCHAR(50) NOT NULL UNIQUE,
    password_hash       VARCHAR(255) NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT FALSE, -- 邮箱验证后为 true
    studentNumber       VARCHAR(255) NOT NULL UNIQUE, -- 学号
    email               VARCHAR(255) NOT NULL UNIQUE,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `verification_tokens`
(
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    email       VARCHAR(255) NOT NULL,       -- 直接存邮箱
    token       VARCHAR(128) NOT NULL,
    type        VARCHAR(30) NOT NULL,        -- e.g., 'REGISTER','RESET'
    expires_at  TIMESTAMP NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX (token),
    INDEX (email)                           -- 根据邮箱查找验证码更快
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

