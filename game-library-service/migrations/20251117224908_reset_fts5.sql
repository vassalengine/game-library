COMMIT TRANSACTION;
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

/* Full-text search */

DROP TABLE projects_fts;
DROP TRIGGER IF EXISTS projects_ai;
DROP TRIGGER IF EXISTS projects_ad;
DROP TRIGGER IF EXISTS projects_au;

CREATE VIRTUAL TABLE IF NOT EXISTS projects_fts USING fts5(
  game_title,
  game_publisher,
  game_year,
  description,
  readme,
  content="projects",
  content_rowid="project_id"
);

/* Set weight for game title to 100 */
INSERT INTO projects_fts(
  projects_fts,
  rank
) VALUES(
  'rank',
  'bm25(100.0)'
);

CREATE TRIGGER IF NOT EXISTS projects_ai AFTER INSERT ON projects
BEGIN
  INSERT INTO projects_fts (
    rowid,
    game_title,
    game_publisher,
    game_year,
    description,
    readme
  )
  VALUES (
    NEW.project_id,
    NEW.game_title,
    NEW.game_publisher,
    NEW.game_year,
    NEW.description,
    NEW.readme
  );
END;

CREATE TRIGGER IF NOT EXISTS projects_ad AFTER DELETE ON projects
BEGIN
  INSERT INTO projects_fts (
    projects_fts,
    rowid,
    game_title,
    game_publisher,
    game_year,
    description,
    readme
  )
  VALUES (
    'delete',
    OLD.project_id,
    OLD.game_title,
    OLD.game_publisher,
    OLD.game_year,
    OLD.description,
    OLD.readme
  );
END;

CREATE TRIGGER IF NOT EXISTS projects_au AFTER UPDATE ON projects
BEGIN
  INSERT INTO projects_fts (
    projects_fts,
    rowid,
    game_title,
    game_publisher,
    game_year,
    description,
    readme
  )
  VALUES (
    'delete',
    OLD.project_id,
    OLD.game_title,
    OLD.game_publisher,
    OLD.game_year,
    OLD.description,
    OLD.readme
  );
  INSERT INTO projects_fts (
    rowid,
    game_title,
    game_publisher,
    game_year,
    description,
    readme
  )
  VALUES (
    NEW.project_id,
    NEW.game_title,
    NEW.game_publisher,
    NEW.game_year,
    NEW.description,
    NEW.readme
  );
END;

INSERT INTO projects_fts(projects_fts) VALUES('rebuild');

COMMIT TRANSACTION;
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
