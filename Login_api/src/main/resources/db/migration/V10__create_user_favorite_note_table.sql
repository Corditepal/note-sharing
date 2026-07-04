CREATE TABLE IF NOT EXISTS user_favorite_note (
    id BIGINT NOT NULL AUTO_INCREMENT,
    user_id BIGINT NOT NULL COMMENT '用户ID',
    note_id BIGINT NOT NULL COMMENT '笔记ID',
    favorite_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '收藏时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_note (user_id, note_id),
    INDEX idx_user_id (user_id),
    INDEX idx_note_id (note_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户收藏笔记关系表';
