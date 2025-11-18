use sqlx::{
    Executor, Transaction,
    sqlite::Sqlite
};

use crate::{
    db::{DatabaseError, map_unique}
};

pub async fn get_publishers<'e, E>(
    ex: E
) -> Result<Vec<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    // return publishers in use now
    Ok(
        sqlx::query_scalar!(
            "
SELECT name
FROM publishers
ORDER BY name COLLATE NOCASE
            "
        )
        .fetch_all(ex)
        .await?
    )
}

pub async fn get_publishers_active<'e, E>(
    ex: E
) -> Result<Vec<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    // return publishers in use now
    Ok(
        sqlx::query_scalar!(
            "
SELECT DISTINCT publishers.name
FROM publishers
JOIN projects
ON publishers.publisher_id = projects.game_publisher_id
ORDER BY publishers.name COLLATE NOCASE
            "
        )
        .fetch_all(ex)
        .await?
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Publisher(pub i64);

pub async fn get_publisher_id<'e, E>(
    ex: E,
    publisher: &str
) -> Result<Option<Publisher>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT publisher_id
FROM publishers
WHERE name = ?
LIMIT 1
            ",
            publisher
        )
        .fetch_optional(ex)
        .await?
        .map(Publisher)
    )
}

pub async fn create_publisher<'e, E>(
    ex: E,
    publisher: &str
) -> Result<Publisher, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_scalar!(
        "
INSERT INTO publishers (name)
VALUES (?)
RETURNING publisher_id
        ",
        publisher
    )
    .fetch_one(ex)
    .await
    .map(Publisher)
    .map_err(map_unique)
}

pub async fn get_or_create_publisher(
    tx: &mut Transaction<'_, Sqlite>,
    publisher: &str
) -> Result<Publisher, DatabaseError>
{
    match get_publisher_id(&mut **tx, publisher).await? {
        Some(publisher) => Ok(publisher),
        None => create_publisher(&mut **tx, publisher).await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_publisher_id_some(pool: Pool) {
        assert_eq!(
            get_publisher_id(&pool, "XYZ").await.unwrap(),
            Some(Publisher(2))
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_tag_id_none(pool: Pool) {
        assert_eq!(
            get_publisher_id(&pool, "bogus").await.unwrap(),
            None
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_publishers_ok(pool: Pool) {
        assert_eq!(
            get_publishers(&pool).await.unwrap(),
            ["ABC".into(), "Test Game Company".to_string(), "XYZ".into()]
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_publishers_active_ok(pool: Pool) {
        assert_eq!(
            get_publishers_active(&pool).await.unwrap(),
            ["Test Game Company".to_string(), "XYZ".into()]
        );
    }

    #[sqlx::test]
    async fn create_publisher_ok(pool: Pool) {
        assert_eq!(
            get_publishers(&pool).await.unwrap(),
            [] as [String; 0]
        );

        let publisher = create_publisher(&pool, "x").await.unwrap();

        assert_eq!(publisher, Publisher(1));
        assert_eq!(
            get_publishers(&pool).await.unwrap(),
            ["x".to_string()]
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn create_publisher_already_exists(pool: Pool) {
        assert_eq!(
            get_publishers(&pool).await.unwrap(),
            ["ABC".to_string(), "Test Game Company".into(), "XYZ".into()]
        );

        assert_eq!(
            create_publisher(&pool, "XYZ").await.unwrap_err(),
            DatabaseError::AlreadyExists
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_or_create_publisher_get(pool: Pool) {
        assert_eq!(
            get_publishers(&pool).await.unwrap(),
            ["ABC".to_string(), "Test Game Company".into(), "XYZ".into()]
        );

        let mut tx = pool.begin().await.unwrap();
        assert_eq!(
            get_or_create_publisher(&mut tx, "XYZ").await.unwrap(),
            Publisher(2)
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_or_create_publisher_create(pool: Pool) {
        assert_eq!(
            get_publishers(&pool).await.unwrap(),
            ["ABC".to_string(), "Test Game Company".into(), "XYZ".into()]
        );

        let mut tx = pool.begin().await.unwrap();
        assert_eq!(
            get_or_create_publisher(&mut tx, "DEF").await.unwrap(),
            Publisher(4)
        );
    }
}
