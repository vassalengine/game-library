use async_trait::async_trait;
use sqlx::{
    Database, Executor, QueryBuilder,
    sqlite::Sqlite
};

use crate::{
    core::CoreError,
    db::DatabaseClient,
    model::{UserData, UserUpdateParams}
};

#[derive(Clone)]
pub struct SqlxDatabaseClient<DB: Database>(pub sqlx::Pool<DB>);

#[async_trait]
impl DatabaseClient for SqlxDatabaseClient<Sqlite> {
    async fn get_user_avatar_template(
        &self,
        username: &str
    ) -> Result<String, CoreError>
    {
        get_user_avatar_template(&self.0, username).await
    }

    async fn get_user_data(
        &self,
        uids: &[i64]
    ) -> Result<Vec<UserData>, CoreError>
    {
        get_user_data(&self.0, uids).await
    }

    async fn update_user(
        &self,
        params: &UserUpdateParams
    ) -> Result<(), CoreError>
    {
        update_user(&self.0, params).await
    }

    async fn create_session(
        &self,
        uid: i64,
        session_id: &str,
        expires: i64
    ) -> Result<(), CoreError>
    {
        create_session(&self.0, uid, session_id, expires).await
    }

    async fn verify_session(
        &self,
        session_id: &str,
        now: i64
    ) -> Result<Option<i64>, CoreError>
    {
        verify_session(&self.0, session_id, now).await
    }

    async fn delete_session(
        &self,
        session_id: &str,
    ) -> Result<(), CoreError>
    {
        delete_session(&self.0, session_id).await
    }
}

async fn get_user_avatar_template<'e, E>(
    ex: E,
    username: &str
) -> Result<String, CoreError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT avatar_template
FROM users
WHERE username = ?
            ",
            username
        )
        .fetch_one(ex)
        .await?
    )
}

async fn get_user_data<'e, E>(
    ex: E,
    uids: &[i64]
) -> Result<Vec<UserData>, CoreError>
where
    E: Executor<'e, Database = Sqlite>
{
    let mut qb: QueryBuilder<E::Database> = QueryBuilder::new(
        "SELECT user_id AS id, username FROM users WHERE user_id IN ("
    );

    let mut qbs = qb.separated(", ");

    for uid in uids {
        qbs.push_bind(uid);
    }

    Ok(
        qb.push(") ORDER BY user_id")
            .build_query_as::<UserData>()
            .fetch_all(ex)
            .await?
    )
}

async fn update_user<'e, E>(
    ex: E,
    params: &UserUpdateParams
) -> Result<(), CoreError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT OR REPLACE INTO users (
    user_id,
    username,
    avatar_template
)
VALUES (?, ?, ?)
        ",
        params.id,
        params.username,
        params.avatar_template
    )
    .execute(ex)
    .await?;

    Ok(())
}

async fn create_session<'e, E>(
    ex: E,
    uid: i64,
    session_id: &str,
    expires: i64
) -> Result<(), CoreError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT OR REPLACE INTO sessions (
    session_id,
    user_id,
    expires
)
VALUES (?, ?, ?)
        ",
        session_id,
        uid,
        expires
    )
    .execute(ex)
    .await?;

    Ok(())
}

async fn verify_session<'e, E>(
    ex: E,
    session_id: &str,
    now: i64
) -> Result<Option<i64>, CoreError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT user_id
FROM sessions
WHERE session_id = ?
AND expires > ?
            ",
            session_id,
            now
        )
        .fetch_optional(ex)
        .await?
    )
}

async fn delete_session<'e, E>(
    ex: E,
    session_id: &str,
) -> Result<(), CoreError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
DELETE FROM sessions
WHERE session_id = ?
        ",
        session_id,
    )
    .execute(ex)
    .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users"))]
    async fn get_user_data_none(pool: Pool) {
        assert_eq!(
            get_user_data(&pool, &[0]).await.unwrap(),
            []
        );
    }


    #[sqlx::test(fixtures("users"))]
    async fn get_user_data_one(pool: Pool) {
        assert_eq!(
            get_user_data(&pool, &[1]).await.unwrap(),
            [
                UserData{ id: 1, username: "alice".into() }
            ]
        );
    }

    #[sqlx::test(fixtures("users"))]
    async fn get_user_data_two(pool: Pool) {
        assert_eq!(
            get_user_data(&pool, &[1, 2]).await.unwrap(),
            [
                UserData{ id: 1, username: "alice".into() },
                UserData{ id: 2, username: "bob".into() }
            ]
        );
    }

    #[sqlx::test(fixtures("users"))]
    async fn get_user_data_many(pool: Pool) {
        let ids = (0..10000).collect::<Vec<_>>();
        assert_eq!(
            get_user_data(&pool, &ids).await.unwrap(),
            [
                UserData{ id: 1, username: "alice".into() },
                UserData{ id: 2, username: "bob".into() }
            ]
       );
    }
}
