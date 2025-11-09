use glc::{
    discourse::UserUpdateParams,
    model::Users
};
use sqlx::{
    Acquire, Executor,
    sqlite::Sqlite
};

use crate::{
    db::DatabaseError,
    model::{Project, User}
};

pub async fn get_user_id<'e, E>(
    ex: E,
    username: &str
) -> Result<Option<User>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT user_id
FROM users
WHERE username = ?
LIMIT 1
            ",
            username
        )
        .fetch_optional(ex)
        .await?
        .map(User)
    )
}

pub async fn get_owners<'e, E>(
    ex: E,
    proj: Project
) -> Result<Users, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        Users {
            users: sqlx::query_scalar!(
                "
SELECT users.username
FROM users
JOIN owners
ON users.user_id = owners.user_id
JOIN projects
ON owners.project_id = projects.project_id
WHERE projects.project_id = ?
ORDER BY users.username
                ",
                proj.0
            )
            .fetch_all(ex)
            .await?
        }
    )
}

pub async fn user_is_owner<'e, E>(
    ex: E,
    user: User,
    proj: Project
) -> Result<bool, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
// FIXME: go back to "SELECT 1 AS present" once sqlx 0.8 is fixed
    Ok(
        sqlx::query_scalar!(
            "
SELECT user_id
FROM owners
WHERE user_id = ? AND project_id = ?
LIMIT 1
            ",
            user.0,
            proj.0
        )
        .fetch_optional(ex)
        .await?
        .is_some()
    )
}

pub async fn add_owner<'e, E>(
    ex: E,
    user: User,
    proj: Project
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT OR IGNORE INTO owners (
    user_id,
    project_id
)
VALUES (?, ?)
        ",
        user.0,
        proj.0
    )
    .execute(ex)
    .await?;

    Ok(())
}

pub async fn add_owners<'a, A>(
    conn: A,
    owners: &Users,
    proj: Project
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    for username in &owners.users {
        // get user id of new owner
        let owner = get_user_id(&mut *tx, username)
            .await?
            .ok_or(DatabaseError::NotFound)?;
        // associate new owner with the project
        add_owner(&mut *tx, owner, proj).await?;
    }

    tx.commit().await?;

    Ok(())
}

pub async fn remove_owner<'e, E>(
    ex: E,
    user: User,
    proj: Project
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
DELETE FROM owners
WHERE user_id = ?
    AND project_id = ?
        ",
        user.0,
        proj.0
    )
    .execute(ex)
    .await?;

    Ok(())
}

pub async fn remove_owners<'a, A>(
    conn: A,
    owners: &Users,
    proj: Project
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    for username in &owners.users {
        // get user id of owner
        let owner = get_user_id(&mut *tx, username)
            .await?
            .ok_or(DatabaseError::NotFound)?;
        // remove old owner from the project
        remove_owner(&mut *tx, owner, proj).await?;
    }

    // prevent removal of last owner
    if !has_owner(&mut *tx, proj).await? {
        return Err(DatabaseError::CannotRemoveLastOwner);
    }

    tx.commit().await?;

    Ok(())
}

pub async fn has_owner<'e, E>(
    ex: E,
    proj: Project
) -> Result<bool, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query!(
            "
SELECT 1 AS present
FROM owners
WHERE project_id = ?
LIMIT 1
            ",
            proj.0
        )
        .fetch_optional(ex)
        .await?
        .is_some()
    )
}

pub async fn update_user<'e, E>(
    ex: E,
    params: &UserUpdateParams
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT OR REPLACE INTO users (
    user_id,
    username
)
VALUES (?, ?)
        ",
        params.id,
        params.username,
    )
    .execute(ex)
    .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn get_owners_ok(pool: Pool) {
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn get_owners_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does.
        assert_eq!(
            get_owners(&pool, Project(0)).await.unwrap(),
            Users { users: vec![] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn user_is_owner_true(pool: Pool) {
        assert!(user_is_owner(&pool, User(1), Project(42)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects","one_owner"))]
    async fn user_is_owner_false(pool: Pool) {
        assert!(!user_is_owner(&pool, User(2), Project(42)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects","one_owner"))]
    async fn user_is_owner_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does.
        assert!(!user_is_owner(&pool, User(2), Project(0)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn add_owner_new(pool: Pool) {
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
        add_owner(&pool, User(2), Project(42)).await.unwrap();
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into()
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn add_owner_existing(pool: Pool) {
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
        add_owner(&pool, User(1), Project(42)).await.unwrap();
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn add_owner_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert!(
            matches!(
                add_owner(&pool, User(1), Project(0)).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn add_owner_not_a_user(pool: Pool) {
        // This should not happen; the User passed in should be good.
        assert!(
            matches!(
                add_owner(&pool, User(0), Project(42)).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        )
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn remove_owner_ok(pool: Pool) {
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
        remove_owner(&pool, User(1), Project(42)).await.unwrap();
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec![] }
        );
    }

    #[sqlx::test(fixtures( "users", "projects", "one_owner"))]
    async fn remove_owner_not_an_owner(pool: Pool) {
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
        remove_owner(&pool, User(2), Project(42)).await.unwrap();
        assert_eq!(
            get_owners(&pool, Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn remove_owner_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does, just a no-op.
        remove_owner(&pool, User(1), Project(0)).await.unwrap();
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn has_owner_yes(pool: Pool) {
        assert!(has_owner(&pool, Project(42)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn has_owner_no(pool: Pool) {
        assert!(!has_owner(&pool, Project(42)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn has_owner_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does.
        assert!(!has_owner(&pool, Project(0)).await.unwrap());
    }
}
