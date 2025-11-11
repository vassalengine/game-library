COMMIT TRANSACTION;
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;


CREATE TABLE IF NOT EXISTS projects_tags_history (
  project_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  added_at INTEGER NOT NULL,
  added_by INTEGER NOT NULL,
  removed_at INTEGER,
  removed_by INTEGER,
  FOREIGN KEY(project_id) REFERENCES projects_history(project_id),
  FOREIGN KEY(tag_id) REFERENCES tags(tag_id),
  FOREIGN KEY(added_by) REFERENCES users(user_id),
  FOREIGN KEY(removed_by) REFERENCES users(user_id),
  CHECK(
    (removed_at IS NULL AND removed_by IS NULL) OR
    (removed_at >= added_at AND removed_by IS NOT NULL)
  )
);

INSERT INTO projects_tags_history SELECT *, 1762897247000000000, 5, NULL, NULL FROM projects_tags;

CREATE TRIGGER IF NOT EXISTS projects_tags_history_ai_start
AFTER INSERT ON projects_tags_history
BEGIN
  INSERT INTO projects_tags (
    project_id,
    tag_id
  )
  VALUES (
    NEW.project_id,
    NEW.tag_id
  );
END;

CREATE TRIGGER IF NOT EXISTS projects_tags_history_au_end
AFTER UPDATE OF removed_at ON projects_tags_history
BEGIN
  DELETE FROM project_tags
  WHERE project_id = OLD.project_id
    AND tag_id = OLD.tag_id;
END;

COMMIT TRANSACTION;
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
