use sqlx::{
    Executor,
    sqlite::Sqlite
};

use crate::{
    db::DatabaseError,
    model::Project
};

pub async fn get_project_tags<'e, E>(
    ex: E,
    proj: Project
) -> Result<Vec<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT tags.tag
FROM projects_tags
JOIN tags
ON projects_tags.tag_id = tags.tag_id
WHERE projects_tags.project_id = ?
ORDER BY tags.tag COLLATE NOCASE
            ",
            proj.0
        )
        .fetch_all(ex)
        .await?
    )
}

pub async fn get_project_tags_at<'e, E>(
    ex: E,
    proj: Project,
    date: i64
) -> Result<Vec<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT tags.tag
FROM projects_tags_history
JOIN tags
ON projects_tags_history.tag_id = tags.tag_id
WHERE projects_tags_history.project_id = ?
    AND projects_tags_history.added_at <= ?
    AND (? < projects_tags_history.removed_at
        OR projects_tags_history.removed_at IS NULL)
ORDER BY tags.tag COLLATE NOCASE
            ",
            proj.0,
            date,
            date
        )
        .fetch_all(ex)
        .await?
    )
}

pub async fn get_tags<'e, E>(
    ex: E
) -> Result<Vec<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT tag
FROM tags
ORDER BY tag COLLATE NOCASE
            "
        )
        .fetch_all(ex)
        .await?
    )
}
