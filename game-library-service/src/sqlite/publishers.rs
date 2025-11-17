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
