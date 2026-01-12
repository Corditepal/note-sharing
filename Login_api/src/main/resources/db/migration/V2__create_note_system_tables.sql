-- 标签表
CREATE TABLE IF NOT EXISTS tags (
                                    id   BIGINT AUTO_INCREMENT PRIMARY KEY,
                                    name VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 笔记空间表
CREATE TABLE IF NOT EXISTS note_spaces (
                                           id          BIGINT AUTO_INCREMENT PRIMARY KEY,
                                           name        VARCHAR(255) NOT NULL,
                                           user_id     BIGINT NOT NULL,
                                           tag_id      BIGINT NOT NULL,          -- 每个空间只有一个标签
                                           created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                           updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                                           CONSTRAINT fk_notespaces_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                                           CONSTRAINT fk_notespaces_tag  FOREIGN KEY (tag_id)  REFERENCES tags(id) ON DELETE RESTRICT,

                                           INDEX idx_notespaces_user_id (user_id),
                                           INDEX idx_notespaces_tag_id  (tag_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 笔记本表
CREATE TABLE IF NOT EXISTS notebooks (
                                         id          BIGINT AUTO_INCREMENT PRIMARY KEY,
                                         name        VARCHAR(255) NOT NULL,
                                         space_id    BIGINT NOT NULL,          -- 不允许为空
                                         tag_id      BIGINT NOT NULL,          -- 每个笔记本只有一个标签
                                         created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                         updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                                         CONSTRAINT fk_notebooks_space FOREIGN KEY (space_id) REFERENCES note_spaces(id) ON DELETE CASCADE,
                                         CONSTRAINT fk_notebooks_tag   FOREIGN KEY (tag_id)   REFERENCES tags(id)        ON DELETE RESTRICT,

                                         INDEX idx_notebooks_space_id (space_id),
                                         INDEX idx_notebooks_tag_id   (tag_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 笔记表
CREATE TABLE IF NOT EXISTS notes (
                                     id           BIGINT AUTO_INCREMENT PRIMARY KEY,
                                     title        VARCHAR(255) NOT NULL,
                                     filename     VARCHAR(255),               -- MinIO 对象的 URL，可用于预览和下载
                                     notebook_id  BIGINT NOT NULL,
                                     file_type    VARCHAR(10) DEFAULT 'MD',
                                     created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                     updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                                     CONSTRAINT fk_notes_notebook FOREIGN KEY (notebook_id) REFERENCES notebooks(id) ON DELETE CASCADE,

                                     INDEX idx_notes_notebook_id (notebook_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
