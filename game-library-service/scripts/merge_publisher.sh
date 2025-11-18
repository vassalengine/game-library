#!/bin/bash -e

src="$1"
dst="$2"

cat << EOF | sqlite3 projects.db
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
UPDATE OR IGNORE projects_data SET game_publisher_id = $dst WHERE game_publisher_id = $src;
UPDATE OR IGNORE projects SET game_publisher_id = publishers.publisher_id, game_publisher = publishers.name FROM (SELECT publisher_id, name FROM publishers WHERE publisher_id = $dst) AS publishers WHERE game_publisher_id = $src;
DELETE FROM publishers WHERE publisher_id = $src;
END TRANSACTION;
EOF
