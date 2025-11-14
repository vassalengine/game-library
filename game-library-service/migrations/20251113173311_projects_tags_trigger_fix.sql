COMMIT TRANSACTION;
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

DROP TRIGGER projects_tags_history_au_end;

CREATE TRIGGER IF NOT EXISTS projects_tags_history_au_end
AFTER UPDATE OF removed_at ON projects_tags_history
BEGIN
  DELETE FROM projects_tags
  WHERE project_id = OLD.project_id
    AND tag_id = OLD.tag_id;
END;

COMMIT TRANSACTION;
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
