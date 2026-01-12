CREATE TABLE IF NOT EXISTS note_stats (
                                          note_id          BIGINT PRIMARY KEY,
                                          author_name      VARCHAR(64) NOT NULL DEFAULT '',
                                          views            BIGINT NOT NULL DEFAULT 0,
                                          likes            BIGINT NOT NULL DEFAULT 0,
                                          favorites        BIGINT NOT NULL DEFAULT 0,
                                          comments         BIGINT NOT NULL DEFAULT 0,
                                          last_activity_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                          version          BIGINT NOT NULL DEFAULT 0,
                                          updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                          CONSTRAINT fk_notestats_note FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 补偿表：当批量落盘重试耗尽时写入
CREATE TABLE IF NOT EXISTS note_stats_compensation (
                                                       id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                                       note_id BIGINT NOT NULL,
                                                       author_name VARCHAR(64) NOT NULL DEFAULT '',
                                                       views BIGINT NOT NULL DEFAULT 0,
                                                       likes BIGINT NOT NULL DEFAULT 0,
                                                       favorites BIGINT NOT NULL DEFAULT 0,
                                                       comments BIGINT NOT NULL DEFAULT 0,
                                                       last_activity_at TIMESTAMP NULL,
                                                       status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                                                       retry_count INT NOT NULL DEFAULT 0,
                                                       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                       updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                                       INDEX (note_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

