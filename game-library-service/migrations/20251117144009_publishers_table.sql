COMMIT TRANSACTION;
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS publishers (
  publisher_id INTEGER PRIMARY KEY NOT NULL CHECK(publisher_id >= 0),
  name TEXT NOT NULL
);

INSERT INTO publishers SELECT DISTINCT NULL, game_publisher FROM projects_data;

CREATE TABLE tmp (
  project_data_id INTEGER PRIMARY KEY NOT NULL CHECK(project_data_id >= 0),
  project_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  slug TEXT NOT NULL,
  description TEXT NOT NULL,
  game_title TEXT NOT NULL,
  game_title_sort TEXT NOT NULL,
  game_publisher_id INTEGER NOT NULL,
  game_year TEXT NOT NULL,
  game_players_min INTEGER CHECK(game_players_min >= 1 OR game_players_min IS NULL),
  game_players_max INTEGER CHECK(game_players_max >= 1 OR game_players_max IS NULL),
  game_length_min INTEGER CHECK(game_length_min >= 1 OR game_length_min IS NULL),
  game_length_max INTEGER CHECK(game_length_max >= 1 OR game_length_max IS NULL),
  readme TEXT NOT NULL,
  image TEXT,
  CHECK(game_players_max >= game_players_min OR game_players_min IS NULL OR game_players_max IS NULL),
  CHECK(game_length_max >= game_length_min OR game_length_min IS NULL OR game_length_max IS NULL),
  FOREIGN KEY(project_id) REFERENCES projects_history(project_id),
  FOREIGN KEY(project_id, image) REFERENCES images(project_id, filename),
  FOREIGN KEY(game_publisher_id) REFERENCES publishers(publisher_id)
);

INSERT INTO tmp SELECT project_data_id, project_id, projects_data.name, slug, description, game_title, game_title_sort, publishers.publisher_id AS game_publisher_id, game_year, game_players_min, game_players_max, game_length_min, game_length_max, readme, image FROM projects_data JOIN publishers ON projects_data.game_publisher == publishers.name;

DROP TABLE projects_data;
ALTER TABLE tmp RENAME TO projects_data;

CREATE TABLE tmp (
  project_id INTEGER PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  slug TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  modified_at INTEGER NOT NULL,
  modified_by INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  description TEXT NOT NULL,
  game_title TEXT NOT NULL,
  game_title_sort TEXT NOT NULL,
  game_publisher_id INTEGER NOT NULL,
  game_publisher TEXT NOT NULL,
  game_year TEXT NOT NULL,
  game_players_min INTEGER CHECK(game_players_min >= 1 OR game_players_min IS NULL),
  game_players_max INTEGER CHECK(game_players_max >= 1 OR game_players_max IS NULL),
  game_length_min INTEGER CHECK(game_length_min >= 1 OR game_length_min IS NULL),
  game_length_max INTEGER CHECK(game_length_max >= 1 OR game_length_max IS NULL),
  readme TEXT NOT NULL,
  image TEXT,
  CHECK(game_players_max >= game_players_min OR game_players_min IS NULL OR game_players_max IS NULL),
  CHECK(game_length_max >= game_length_min OR game_length_min IS NULL OR game_length_max IS NULL),
  UNIQUE(name),
  UNIQUE(normalized_name),
  UNIQUE(slug),
  FOREIGN KEY(project_id) REFERENCES projects_history(project_id),
  FOREIGN KEY(project_id, image) REFERENCES images(project_id, filename),
  FOREIGN KEY(modified_by) REFERENCES users(user_id),
  FOREIGN KEY(project_id, revision) REFERENCES projects_revisions(project_id, revision),
  FOREIGN KEY(game_publisher_id) REFERENCES publishers(publisher_id)
);

INSERT INTO tmp SELECT project_id, projects.name, normalized_name, slug, created_at, modified_at, modified_by, revision, description, game_title, game_title_sort, publishers.publisher_id AS game_publisher_id, game_publisher, game_year, game_players_min, game_players_max, game_length_min, game_length_max, readme, image FROM projects JOIN publishers ON projects.game_publisher == publishers.name;

DROP TABLE projects;
ALTER TABLE tmp RENAME TO projects;

COMMIT TRANSACTION;
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
