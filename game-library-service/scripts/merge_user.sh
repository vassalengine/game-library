#!/bin/bash -e

src="$1"
dst="$2"

cat << EOF | sqlite3 projects.db
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
UPDATE OR IGNORE owners SET user_id = $dst WHERE user_id = $src;
UPDATE OR IGNORE players SET user_id = $dst WHERE user_id = $src;
UPDATE packages SET created_by = $dst WHERE created_by = $src;
UPDATE packages_history SET created_by = $dst WHERE created_by = $src;
UPDATE packages_history SET deleted_by = $dst WHERE deleted_by = $src;
UPDATE packages_revisions SET modified_by = $dst WHERE modified_by = $src;
UPDATE releases SET published_by = $dst WHERE published_by = $src;
UPDATE releases_history SET published_by = $dst WHERE published_by = $src;
UPDATE releases_history SET deleted_by = $dst WHERE deleted_by = $src;
UPDATE files SET published_by = $dst WHERE published_by = $src;
UPDATE images SET published_by = $dst WHERE published_by = $src;
UPDATE image_revisions SET published_by = $dst WHERE published_by = $src;
UPDATE galleries SET published_by = $dst WHERE published_by = $src;
UPDATE galleries_history SET published_by = $dst WHERE published_by = $src;
UPDATE galleries_history SET removed_by = $dst WHERE removed_by = $src;
UPDATE projects SET modified_by = $dst WHERE modified_by = $src;
UPDATE projects_revisions SET modified_by = $dst WHERE modified_by = $src;
UPDATE projects_tags_history SET added_by = $dst WHERE added_by = $src;
UPDATE projects_tags_history SET removed_by = $dst WHERE removed_by = $src;
UPDATE flags SET user_id = $dst WHERE user_id = $src;
DELETE FROM users WHERE user_id = $src;
END TRANSACTION;
EOF
