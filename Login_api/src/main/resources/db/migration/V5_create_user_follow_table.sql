CREATE TABLE IF NOT EXISTS user_follow (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    follower_id BIGINT NOT NULL COMMENT '关注者用户ID',
    followee_id BIGINT NOT NULL COMMENT '被关注者用户ID',
    follow_time DATETIME NOT NULL COMMENT '关注时间',

    UNIQUE KEY uk_follower_followee (follower_id, followee_id),
    INDEX idx_follower_id (follower_id),
    INDEX idx_followee_id (followee_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户关注关系表';

CREATE TABLE IF NOT EXISTS conversation_relation (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    conversation_id VARCHAR(64) NOT NULL COMMENT '对应 MongoDB Conversation _id',
    user1_id BIGINT NOT NULL COMMENT '用户ID1，约定小ID',
    user2_id BIGINT NOT NULL COMMENT '用户ID2，约定大ID',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_pair (user1_id, user2_id),
    UNIQUE KEY uk_conversation_id (conversation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话关系表';
