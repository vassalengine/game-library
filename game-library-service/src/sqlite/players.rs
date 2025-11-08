use glc::model::Users;
use sqlx::{
    Executor,
    sqlite::Sqlite
};

use crate::{
    db::DatabaseError,
    model::{Project, User}
};

pub async fn get_players<'e, E>(
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
JOIN players
ON users.user_id = players.user_id
JOIN projects
ON players.project_id = projects.project_id
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

pub async fn add_player<'e, E>(
    ex: E,
    user: User,
    proj: Project
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT OR IGNORE INTO players (
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

pub async fn remove_player<'e, E>(
    ex: E,
    user: User,
    proj: Project
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
DELETE FROM players
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

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn get_players_ok(pool: Pool) {
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into()
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn get_players_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does.
        assert_eq!(
            get_players(&pool, Project(0)).await.unwrap(),
            Users { users: vec![] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn add_player_new(pool: Pool) {
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                ]
            }
        );
        add_player(&pool, User(3), Project(42)).await.unwrap();
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                    "chuck".into()
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn add_player_existing(pool: Pool) {
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                ]
            }
        );
        add_player(&pool, User(2), Project(42)).await.unwrap();
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn add_player_not_a_project(pool: Pool) {
        assert!(
            matches!(
                add_player(&pool, User(2), Project(0)).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn add_player_not_a_user(pool: Pool) {
        assert!(
            matches!(
                add_player(&pool, User(0), Project(42)).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn remove_player_existing(pool: Pool) {
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                ]
            }
        );
        remove_player(&pool, User(2), Project(42)).await.unwrap();
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "bob".into(),
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn remove_player_not_a_player(pool: Pool) {
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                ]
            }
        );
        remove_player(&pool, User(3), Project(42)).await.unwrap();
        assert_eq!(
            get_players(&pool, Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn remove_player_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        // However, it's not an error if it does, just a no-op.
        remove_player(&pool, User(3), Project(0)).await.unwrap();
    }
}
