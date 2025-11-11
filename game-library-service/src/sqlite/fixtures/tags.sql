INSERT INTO tags (tag_id, tag)
VALUES
  (1, "a"),
  (2, "b");

INSERT INTO projects_tags_history (
  project_id,
  tag_id,
  added_at,
  added_by,
  removed_at,
  removed_by
)
VALUES
  (6, 1, 1762897247000000000, 1, NULL, NULL),
  (6, 2, 1762897247000000000, 2, NULL, NULL),
  (42, 1, 1762897247000000000, 1, NULL, NULL);
