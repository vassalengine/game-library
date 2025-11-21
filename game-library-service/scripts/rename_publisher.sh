#!/bin/bash -e

pid="$1"
name="$2"

cat << EOF | sqlite3 projects.db
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
UPDATE projects SET game_publisher = 'name' WHERE game_publisher_id = $pid;
UPDATE publishers SET name = '$name' WHERE publisher_id = $pid;
END TRANSACTION;
EOF
