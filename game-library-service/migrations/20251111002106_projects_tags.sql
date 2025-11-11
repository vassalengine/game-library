COMMIT TRANSACTION;
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

DROP TABLE tmp;

CREATE TABLE tmp (
  tag_id INTEGER PRIMARY KEY NOT NULL CHECK(tag_id >= 0),
  tag TEXT NOT NULL CHECK(tag != ""),
  UNIQUE(tag)
);

INSERT INTO tmp SELECT DISTINCT NULL, tag FROM tags ORDER BY tag;

CREATE TABLE IF NOT EXISTS projects_tags (
  project_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(project_id),
  FOREIGN KEY(tag_id) REFERENCES tags(tag_id),
  UNIQUE(project_id, tag_id)
);

INSERT INTO projects_tags SELECT tags.project_id, tmp.tag_id FROM tmp JOIN tags ON tmp.tag = tags.tag;

DROP TABLE tags;
ALTER TABLE tmp RENAME TO tags;

COMMIT TRANSACTION;
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
