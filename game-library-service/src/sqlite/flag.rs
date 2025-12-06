use glc::model::FlagTag;
use sqlx::{
    Executor,
    sqlite::Sqlite
};

use crate::{
    db::{DatabaseError, FlagRow},
    input::FlagPost,
    model::{Admin, Flag, Project, User},
    sqlite::require_one_modified
};

impl<'a> From<&'a FlagPost> for (u32, Option<&'a str>) {
    fn from(f: &FlagPost) -> (u32, Option<&str>) {
        match f {
            FlagPost::Inappropriate => (0, None),
            FlagPost::Spam => (1, None),
            FlagPost::Illegal(msg) => (2, Some(msg)),
            FlagPost::Other(msg) => (3, Some(msg))
        }
    }
}

fn flag_tag(f: i64) -> FlagTag {
    match f {
        0 => FlagTag::Inappropriate,
        1 => FlagTag::Spam,
        2 => FlagTag::Illegal,
         _ => FlagTag::Other
    }
}

struct RawFlagRow {
    pub flag_id: i64,
    pub project: String,
    pub slug: String,
    pub flag: i64,
    pub flagged_at: i64,
    pub flagged_by: String,
    pub message: Option<String>
}

impl From<RawFlagRow> for FlagRow {
    fn from(r: RawFlagRow) -> Self {
        Self {
           flag_id: r.flag_id,
           project: r.project,
           slug: r.slug,
           flag: flag_tag(r.flag),
           flagged_at: r.flagged_at,
           flagged_by: r.flagged_by,
           message: r.message
        }
    }
}

pub async fn get_flag_id<'e, E>(
    ex: E,
    flag: i64
) -> Result<Option<Flag>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT flag_id
FROM flags
WHERE flag_id = ?
LIMIT 1
            ",
            flag
        )
        .fetch_optional(ex)
        .await?
        .map(Flag)
    )
}

pub async fn add_flag<'e, E>(
    ex: E,
    reporter: User,
    proj: Project,
    flag: &FlagPost,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    let (flag, msg) = flag.into();

    sqlx::query!(
        "
INSERT INTO flags (
    user_id,
    project_id,
    flagged_at,
    flag,
    message
)
VALUES (?, ?, ?, ?, ?)
        ",
        reporter.0,
        proj.0,
        now,
        flag,
        msg
    )
    .execute(ex)
    .await?;

    Ok(())
}

pub async fn get_flags<'e, E>(
    ex: E
) -> Result<Vec<FlagRow>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_as!(
            RawFlagRow,
            "
SELECT
    flags.flag_id,
    projects.name AS project,
    projects.slug,
    flags.flag,
    flags.flagged_at,
    users.username AS flagged_by,
    flags.message
FROM flags
JOIN users
ON flags.user_id = users.user_id
JOIN projects
ON flags.project_id = projects.project_id
WHERE closed_at IS NULL
ORDER BY flags.flag_id
            "
        )
        .fetch_all(ex)
        .await?
        .into_iter()
        .map(FlagRow::from)
        .collect()
    )
}

pub async fn close_flag<'e, E>(
    ex: E,
    admin: Admin,
    flag: Flag,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
UPDATE flags
SET
    closed_at = ?,
    closed_by = ?
WHERE flag_id = ?
        ",
        now,
        admin.0,
        flag.0
    )
    .execute(ex)
    .await
    .map_err(DatabaseError::from)
    .and_then(require_one_modified)
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{slice, sync::LazyLock};

    type Pool = sqlx::Pool<Sqlite>;

    #[test]
    fn tuple_from_flag_post_inappropriate() {
        let (t, m): (u32, Option<&str>) = (&FlagPost::Inappropriate).into();
        assert_eq!((t, m), (0, None));
    }

    #[test]
    fn tuple_from_flag_post_spam() {
        let (t, m): (u32, Option<&str>) = (&FlagPost::Spam).into();
        assert_eq!((t, m), (1, None));
    }

    #[test]
    fn tuple_from_flag_post_illegal() {
        let f = FlagPost::Illegal("x".into());
        let (t, m): (u32, Option<&str>) = (&f).into();
        assert_eq!((t, m), (2, Some("x")));
    }

    #[test]
    fn tuple_from_flag_post_other() {
        let f = FlagPost::Other("x".into());
        let (t, m): (u32, Option<&str>) = (&f).into();
        assert_eq!((t, m), (3, Some("x")));
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn add_flag_ok(pool: Pool) {
        assert_eq!(
            get_flags(&pool).await.unwrap(),
            []
        );

        add_flag(
            &pool,
            User(1),
            Project(42),
            &FlagPost::Spam,
            1702569006419538068
        ).await.unwrap();

        assert_eq!(
            get_flags(&pool).await.unwrap(),
            [
                FlagRow {
                    flag_id: 1,
                    project: "test_game".into(),
                    slug: "test_game".into(),
                    flag: FlagTag::Spam,
                    flagged_at: 1702569006419538068,
                    flagged_by: "bob".into(),
                    message: None
                }
            ]
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn add_flag_not_a_user(pool: Pool) {
        // This should not happen; the User passed in should be good.
        assert!(
            matches!(
                add_flag(
                    &pool,
                    User(0),
                    Project(42),
                    &FlagPost::Spam,
                    1702569006419538068
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn add_flag_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert!(
            matches!(
                add_flag(
                    &pool,
                    User(1),
                    Project(0),
                    &FlagPost::Spam,
                    1702569006419538068
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    static FLAG_ONE: LazyLock<FlagRow> = LazyLock::new(||
        FlagRow {
            flag_id: 1,
            project: "test_game".into(),
            slug: "test_game".into(),
            flag: FlagTag::Spam,
            flagged_at: 1699804206419538067,
            flagged_by: "bob".into(),
            message: None
        }
    );

    #[sqlx::test(fixtures("users", "projects", "flags"))]
    async fn get_flags_ok(pool: Pool) {
        assert_eq!(
            &get_flags(&pool).await.unwrap(),
            slice::from_ref(&*FLAG_ONE)
        );
    }

    #[sqlx::test(fixtures("users", "projects", "flags"))]
    async fn close_flag_ok(pool: Pool) {
        assert_eq!(
            &get_flags(&pool).await.unwrap(),
            slice::from_ref(&*FLAG_ONE)
        );

        close_flag(
            &pool,
            Admin(1),
            Flag(1),
            1702569006419538068
        ).await.unwrap();

        assert_eq!(
            get_flags(&pool).await.unwrap(),
            []
        );
    }

    #[sqlx::test(fixtures("users", "projects", "flags"))]
    async fn close_flag_not_a_flag(pool: Pool) {
        // This should not happen; the Flag passed in should be good.
        assert_eq!(
            close_flag(
                 &pool,
                 Admin(1),
                 Flag(0),
                 1702569006419538068
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "flags"))]
    async fn close_flag_not_a_user(pool: Pool) {
        // This should not happen; the Admin passed in should be good.
        assert!(
            matches!(
                close_flag(
                    &pool,
                    Admin(0),
                    Flag(1),
                    1702569006419538068
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }
}
