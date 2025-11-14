use sqlx::{
    Acquire, Executor,
    sqlite::Sqlite
};

use crate::{
    db::{DatabaseError, map_unique},
    model::{Owner, Project},
    sqlite::{
        require_one_modified,
        project::update_project_non_project_data
    }
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Tag(i64);

pub async fn get_tag_id<'e, E>(
    ex: E,
    tag: &str
) -> Result<Option<Tag>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT tag_id
FROM tags
WHERE tag = ?
LIMIT 1
            ",
            tag
        )
        .fetch_optional(ex)
        .await?
        .map(Tag)
    )
}

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

async fn project_has_tag<'e, E>(
    ex: E,
    proj: Project,
    tag: Tag,
) -> Result<bool, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT COUNT(1)
FROM projects_tags
WHERE project_id = ?
    AND tag_id = ?
LIMIT 1
            ",
            proj.0,
            tag.0
        )
        .fetch_optional(ex)
        .await?
        .is_some()
    )
}

async fn create_project_tag_row<'e, E>(
    ex: E,
    owner: Owner,
    proj: Project,
    tag: Tag,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT INTO projects_tags_history (
    project_id,
    tag_id,
    added_at,
    added_by
)
VALUES (?, ?, ?, ?)
        ",
        proj.0,
        tag.0,
        now,
        owner.0
    )
    .execute(ex)
    .await
    .map_err(map_unique)?;

    Ok(())
}

async fn retire_project_tag_row<'e, E>(
    ex: E,
    owner: Owner,
    proj: Project,
    tag: Tag,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
UPDATE projects_tags_history
SET
    removed_by = ?,
    removed_at = ?
WHERE project_id = ?
    AND tag_id = ?
        ",
        owner.0,
        now,
        proj.0,
        tag.0
    )
    .execute(ex)
    .await
    .map_err(DatabaseError::from)
    .and_then(require_one_modified)
}

pub async fn update_project_tags<'a, A, S>(
    conn: A,
    owner: Owner,
    proj: Project,
    tags_add: &[S],
    tags_remove: &[S],
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>,
    S: AsRef<str>
{
    let mut tx = conn.begin().await?;

    let mut changed = false;

    for tname in tags_remove {
        let tag = get_tag_id(&mut *tx, tname.as_ref()).await?
            .ok_or(DatabaseError::NotFound)?;
        retire_project_tag_row(&mut *tx, owner, proj, tag, now).await?;
        changed = true;
    }

    for tname in tags_add {
        let tag = get_tag_id(&mut *tx, tname.as_ref()).await?
            .ok_or(DatabaseError::NotFound)?;

        if !project_has_tag(&mut *tx, proj, tag).await? {
            create_project_tag_row(&mut *tx, owner, proj, tag, now).await?;
            changed = true;
        }
    }

    if changed {
        // update project to reflect the change
        update_project_non_project_data(&mut tx, owner, proj, now).await?;
    }

    tx.commit().await?;

    Ok(())
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

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_tags_ok(pool: Pool) {
        assert_eq!(
            get_tags(&pool).await.unwrap(),
            ["a".to_string(), "b".into()]
        );
    }
}
