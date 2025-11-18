use sqlx::{
    Acquire, Executor, Transaction,
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
pub struct Tag(pub i64);

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

async fn create_tag<'e, E>(
    ex: E,
    tag: &str
) -> Result<Tag, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_scalar!(
        "
INSERT INTO tags (tag)
VALUES (?)
RETURNING tag_id
        ",
        tag
    )
    .fetch_one(ex)
    .await
    .map(Tag)
    .map_err(map_unique)
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

pub async fn get_tags_active<'e, E>(
    ex: E
) -> Result<Vec<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    // return tags in use now
    Ok(
        sqlx::query_scalar!(
            "
SELECT DISTINCT tags.tag
FROM tags
JOIN projects_tags
ON tags.tag_id = projects_tags.tag_id
ORDER BY tag COLLATE NOCASE
            "
        )
        .fetch_all(ex)
        .await?
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
    let count = sqlx::query_scalar!(
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
    .fetch_one(ex)
    .await?;

    Ok(count == 1)
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


pub async fn update_project_tags_in_trans<S>(
    tx: &mut Transaction<'_, Sqlite>,
    owner: Owner,
    proj: Project,
    tags_add: &[S],
    tags_remove: &[S],
    now: i64
) -> Result<bool, DatabaseError>
where
    S: AsRef<str>
{
    let mut changed = false;

    // remove tags
    for tname in tags_remove {
        // remove tag only if project was tagged
        if let Some(tag) = get_tag_id(&mut **tx, tname.as_ref()).await? {
            if project_has_tag(&mut **tx, proj, tag).await? {
                retire_project_tag_row(&mut **tx, owner, proj, tag, now).await?;
                changed = true;
            }
        }
    }

    // add tags
    for tname in tags_add {
        let tag = get_tag_id(&mut **tx, tname.as_ref()).await?;

        let tag = if let Some(tag) = tag {
            if project_has_tag(&mut **tx, proj, tag).await? {
                // project already tagged, don't add it
                continue;
            }
            tag
        }
        else {
            // new tag, create it
// TODO: reenable when we permit tag creation
//            create_tag(&mut **tx, tname.as_ref()).await?
            return Err(DatabaseError::NotFound);
        };

        create_project_tag_row(&mut **tx, owner, proj, tag, now).await?;
        changed = true;
    }

    Ok(changed)
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

    let changed = update_project_tags_in_trans(
        &mut tx,
        owner,
        proj,
        tags_add,
        tags_remove,
        now
    ).await?;

    if changed {
        // update project to reflect the change
        update_project_non_project_data(&mut tx, owner, proj, now).await?;
        tx.commit().await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_tag_id_some(pool: Pool) {
        assert_eq!(
            get_tag_id(&pool, "a").await.unwrap(),
            Some(Tag(1))
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_tag_id_none(pool: Pool) {
        assert_eq!(
            get_tag_id(&pool, "d").await.unwrap(),
            None
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_tags_active_ok(pool: Pool) {
        assert_eq!(
            get_tags_active(&pool).await.unwrap(),
            ["a".to_string(), "b".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_tags_ok(pool: Pool) {
        assert_eq!(
            get_tags(&pool).await.unwrap(),
            ["a".to_string(), "b".into(), "c".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn create_tag_ok(pool: Pool) {
        assert_eq!(
            get_tags(&pool).await.unwrap(),
            [] as [String; 0]
        );

        create_tag(&pool, "c").await.unwrap();

        assert_eq!(
            get_tags(&pool).await.unwrap(),
            ["c".to_string()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn create_tag_already_exists(pool: Pool) {
        assert_eq!(
            get_tags(&pool).await.unwrap(),
            ["a".to_string(), "b".into(), "c".into()]
        );

        assert_eq!(
            create_tag(&pool, "a").await.unwrap_err(),
            DatabaseError::AlreadyExists
        );
    }

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
    async fn project_has_tag_yes(pool: Pool) {
        assert!(project_has_tag(&pool, Project(6), Tag(1)).await.unwrap());
    }

   #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn project_has_tag_no(pool: Pool) {
        assert!(!project_has_tag(&pool, Project(6), Tag(4)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn project_has_tag_not_a_project_ok(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does.
        assert!(!project_has_tag(&pool, Project(0), Tag(1)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_remove(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

        update_project_tags(
            &pool,
            Owner(1),
            Project(6),
            &[],
            &["a".to_string()],
            1762897247000000001
        ).await.unwrap();

        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["b".to_string()]
        );
    }

// TODO: reenable when we permit tag creation
/*
    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_add_new_tag(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

        update_project_tags(
            &pool,
            Owner(1),
            Project(6),
            &["d".to_string()],
            &[],
            1762897247000000001
        ).await.unwrap();

        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into(), "d".into()]
        );
    }
*/

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_add_existing_tag(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

        update_project_tags(
            &pool,
            Owner(1),
            Project(6),
            &["c".to_string()],
            &[],
            1762897247000000001
        ).await.unwrap();

        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into(), "c".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_add_remove(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

        update_project_tags(
            &pool,
            Owner(1),
            Project(6),
            &["c".to_string()],
            &["a".into()],
            1762897247000000001
        ).await.unwrap();

        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["b".to_string(), "c".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_add_already_has(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

        update_project_tags(
            &pool,
            Owner(1),
            Project(6),
            &["a".to_string()],
            &[],
            1762897247000000001
        ).await.unwrap();

        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_remove_does_not_have(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

        update_project_tags(
            &pool,
            Owner(1),
            Project(6),
            &[],
            &["c".to_string()],
            1762897247000000001
        ).await.unwrap();

        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_no_changes(pool: Pool) {
        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );

        update_project_tags(
            &pool,
            Owner(1),
            Project(6),
            &([] as [String; 0]),
            &[],
            0
        ).await.unwrap();

        assert_eq!(
            get_project_tags(&pool, Project(6)).await.unwrap(),
            ["a".to_string(), "b".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_not_an_owner(pool: Pool) {
        // This should not happen; the Owner passed in should be good.
        assert!(matches!(
            update_project_tags(
                &pool,
                Owner(0),
                Project(6),
                &["foo".to_string()],
                &[],
                0
            ).await.unwrap_err(),
// TODO: reenable when we permit tag creation
//            DatabaseError::SqlxError(_)
            DatabaseError::NotFound
        ));
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn update_project_tags_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert!(matches!(
            update_project_tags(
                &pool,
                Owner(1),
                Project(0),
                &["foo".to_string()],
                &[],
                0
            ).await.unwrap_err(),
// TODO: reenable when we permit tag creation
//            DatabaseError::SqlxError(_)
            DatabaseError::NotFound
        ));
    }
}
