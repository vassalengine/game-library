INSERT INTO projects_history (
  project_id,
  created_at
)
VALUES
  (1, 0),
  (2, 0),
  (3, 0),
  (4, 0),
  (5, 0),
  (6, 0),
  (7, 0),
  (8, 0),
  (9, 0),
  (10, 0);

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
  (1, 1, "a", "a", "a", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (2, 2, "b", "b", "b", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (3, 3, "c", "c", "c",  "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (4, 4, "d", "d", "d", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (5, 5, "e", "e", "e", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (6, 6, "f", "f", "f", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (7, 7, "g", "g", "g", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (8, 8, "h", "h", "h", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (9, 9, "i", "i", "i", "", "", 1, "", NULL, NULL, NULL, NULL, "", NULL),
  (10, 10, "j", "j", "j","", "", 1, "", NULL, NULL, NULL, NULL, "", NULL);

INSERT INTO projects_revisions (
  project_id,
  modified_at,
  modified_by,
  revision,
  project_data_id
)
VALUES
  (1, 1, 1, 1, 1),
  (2, 2, 1, 1, 2),
  (3, 3, 1, 1, 3),
  (4, 4, 1, 1, 4),
  (5, 5, 1, 1, 5),
  (6, 6, 1, 1, 6),
  (7, 7, 1, 1, 7),
  (8, 8, 1, 1, 8),
  (9, 9, 1, 1, 9),
  (10, 10, 1, 1, 10);

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
  (1, "a", "a", "a", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 1, 1, 1),
  (2, "b", "b", "b", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 2, 1, 1),
  (3, "c", "c", "c", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 3, 1, 1),
  (4, "d", "d", "d", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 4, 1, 1),
  (5, "e", "e", "e", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 5, 1, 1),
  (6, "f", "f", "f", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 6, 1, 1),
  (7, "g", "g", "g", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 7, 1, 1),
  (8, "h", "h", "h", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 8, 1, 1),
  (9, "i", "i", "i", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 9, 1, 1),
  (10, "j", "j", "j", 0, "", "", "", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 10, 1, 1);
