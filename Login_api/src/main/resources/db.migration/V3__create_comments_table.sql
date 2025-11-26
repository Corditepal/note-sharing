CREATE TABLE IF NOT EXISTS comments (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  content TEXT NOT NULL,
  user_id BIGINT NOT NULL,
  note_id BIGINT NOT NULL,
  parent_id BIGINT NULL,
  likes INT NOT NULL DEFAULT 0,
  replies INT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL,
  CONSTRAINT fk_comments_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_comments_note FOREIGN KEY (note_id) REFERENCES notes(id),
  CONSTRAINT fk_comments_parent FOREIGN KEY (parent_id) REFERENCES comments(id)
);