#!/bin/bash -e

proj_id="$1"

cat << EOF | sqlite3 projects.db
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
DELETE FROM flags WHERE project_id = $proj_id;
DELETE FROM projects_tags WHERE project_id = $proj_id;
DELETE FROM image_revisions WHERE project_id = $proj_id;
DELETE FROM galleries WHERE project_id = $proj_id;
DELETE FROM players WHERE project_id = $proj_id;
DELETE FROM owners WHERE project_id = $proj_id;
DELETE FROM files WHERE release_id IN (SELECT releases.release_id FROM releases JOIN packages ON releases.package_id = packages.package_id WHERE packages.project_id = $proj_id);
DELETE FROM releases WHERE package_id IN (SELECT package_id FROM packages WHERE project_id = $proj_id);
DELETE FROM releases_history WHERE package_id IN (SELECT package_id FROM packages_history WHERE project_id = $proj_id);
DELETE FROM packages WHERE project_id = $proj_id;
DELETE FROM packages_revisions WHERE package_id IN (SELECT package_id FROM packages_history WHERE project_id = $proj_id);
DELETE FROM packages_history WHERE project_id = $proj_id;
UPDATE projects SET image = NULL WHERE project_id = $proj_id;
UPDATE projects_data SET image = NULL WHERE project_id = $proj_id;
DELETE FROM images WHERE project_id = $proj_id;
DELETE FROM projects WHERE project_id = $proj_id;
DELETE FROM projects_revisions WHERE project_id = $proj_id;
DELETE FROM projects_data WHERE project_id = $proj_id;
DELETE FROM projects_history WHERE project_id = $proj_id;
COMMIT TRANSACTION;
EOF
