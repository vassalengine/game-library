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

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_project_tags_ok(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_project_tags_not_a_project_ok(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does.
        assert_eq!(
            get_project_tags(&pool, Project(0)).await.unwrap(),
            [] as [String; 0]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_project_tags_at_ok(pool: Pool) {
        assert_eq!(
            get_project_tags_at(&pool, Project(6), 1762897247000000001)
                .await
                .unwrap(),
            ["a".to_string(), "b".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_project_tags_at_not_a_project_ok(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does.
        assert_eq!(
            get_project_tags_at(&pool, Project(0), 0).await.unwrap(),
            [] as [String; 0]
        );
    }

}
