INSERT INTO projects_history (
  project_id,
  created_at
)
VALUES
  (1, 0),
  (2, 0),
  (3, 0),
  (4, 0);

INSERT INTO publishers (
  publisher_id,
  name
)
VALUES
  (1, "");

INSERT INTO projects_data (
  project_data_id,
  project_id,
  name,
  slug,
  description,
  game_title,
  game_title_sort,
  game_publisher_id,
  game_year,
  game_players_min,
  game_players_max,
  game_length_min,
  game_length_max,
  readme,
  image
)
VALUES
  (1, 1, "a", "a", "", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (2, 2, "b", "b", "", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (3, 3, "c", "c", "", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (4, 4, "d", "d", "", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL);

INSERT INTO projects_revisions (
  project_id,
  modified_at,
  modified_by,
  revision,
  project_data_id
)
VALUES
  (1, 0, 1, 1, 1),
  (2, 0, 1, 1, 2),
  (3, 0, 1, 1, 3),
  (4, 0, 1, 1, 4);

INSERT INTO projects (
  project_id,
  name,
  normalized_name,
  slug,
  created_at,
  description,
  game_title,
  game_title_sort,
  game_publisher_id,
  game_publisher,
  game_year,
  game_players_min,
  game_players_max,
  game_length_min,
  game_length_max,
  readme,
  image,
  modified_at,
  modified_by,
  revision
)
VALUES
  (1, "a", "a", "a", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 0, 1, 1),
  (2, "b", "b", "b", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 0, 1, 1),
  (3, "c", "c", "c", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 0, 1, 1),
  (4, "d", "d", "d", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 0, 1, 1);
