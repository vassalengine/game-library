INSERT INTO projects_history (
  project_id,
  created_at
)
VALUES
  (
    42,
    1699804206419538067
  ),
  (
    6,
    1573573806419538067
  );

INSERT INTO publishers (
  publisher_id,
  name
)
VALUES
  (
    1,
    "Test Game Company"
  ),
  (
    2,
    "XYZ"
  );

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
  (
    1,
    42,
    "test_game",
    "test_game",
    "Brian's Trademarked Game of Being a Test Case",
    "A Game of Tests",
    "game of tests, a",
    1,
    "1978",
    NULL,
    3,
    NULL,
    NULL,
    "",
    NULL
  ),
  (
    2,
    42,
    "test_game",
    "test_game",
    "Brian's Trademarked Game of Being a Test Case",
    "A Game of Tests",
    "game of tests, a",
    1,
    "1979",
    NULL,
    3,
    NULL,
    NULL,
    "",
    NULL
  ),
  (
    3,
    6,
    "a_game",
    "a_game",
    "Another game",
    "Some Other Game",
    "some other game",
    2,
    "1993",
    NULL,
    NULL,
    NULL,
    NULL,
    "",
    NULL
  );
;

INSERT INTO projects_revisions (
  project_id,
  modified_at,
  modified_by,
  revision,
  project_data_id
)
VALUES
  (
    42,
    1699804206419538067,
    1,
    1,
    1
  ),
  (
    42,
    1702569006419538067,
    1,
    3,
    2
  ),
  (
    6,
    1573573806419538067,
    1,
    1,
    3
  );

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
  (
    42,
    "test_game",
    "test_game",
    "test_game",
    1699804206419538067,
    "Brian's Trademarked Game of Being a Test Case",
    "A Game of Tests",
    "game of tests, a",
    1,
    "Test Game Company",
    "1979",
    NULL,
    3,
    NULL,
    NULL,
    "",
    NULL,
    1702569006419538067,
    1,
    3
  ),
  (
    6,
    "a_game",
    "a_game",
    "a_game",
    1573573806419538067,
    "Another game",
    "Some Other Game",
    "some other game",
    2,
    "XYZ",
    "1993",
    NULL,
    NULL,
    NULL,
    NULL,
    "",
    NULL,
    1573573806419538067,
    1,
    1
  );
