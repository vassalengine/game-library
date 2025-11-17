INSERT INTO publishers (
  publisher_id,
  name
)
VALUES
  (1, "");

INSERT INTO projects (
  project_id,
  name,
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
  (1, "a", 0, "", "", "a", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 1, 1, 1),
  (2, "b", 0, "", "", "a", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 2, 1, 1),
  (3, "c", 0, "", "", "b", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 3, 1, 1),
  (4, "d", 0, "", "", "c", 1, "", "", NULL, NULL, NULL, NULL, "", NULL, 4, 1, 1);
