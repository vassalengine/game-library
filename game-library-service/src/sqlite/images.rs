use glc::model::GalleryImage;
use num_bigint::BigUint;
use sqlx::{
    Acquire, Executor, Transaction,
    sqlite::Sqlite
};
use std::collections::HashMap;

use crate::{
    db::DatabaseError,
    input::{GalleryOp, GalleryPatch},
    model::{Owner, Project},
    sqlite::{
        require_one_modified,
        project::update_project_non_project_data
    }
};

pub async fn get_image_url<'e, E>(
    ex: E,
    proj: Project,
    img_name: &str
) -> Result<Option<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT url
FROM images
WHERE project_id = ?
    AND filename = ?
LIMIT 1
            ",
            proj.0,
            img_name
        )
        .fetch_optional(ex)
        .await?
    )
}

pub async fn get_image_url_at<'e, E>(
    ex: E,
    proj: Project,
    img_name: &str,
    date: i64
) -> Result<Option<String>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT url
FROM image_revisions
WHERE project_id = ?
    AND filename = ?
    AND published_at <= ?
ORDER BY published_at DESC
LIMIT 1
            ",
            proj.0,
            img_name,
            date
        )
        .fetch_optional(ex)
        .await?
    )
}

async fn create_image_revision_row<'e, E>(
    ex: E,
    owner: Owner,
    proj: Project,
    img_name: &str,
    url: &str,
    content_type: &str,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT INTO image_revisions (
    project_id,
    filename,
    url,
    content_type,
    published_at,
    published_by
)
VALUES (?, ?, ?, ?, ?, ?)
        ",
        proj.0,
        img_name,
        url,
        content_type,
        now,
        owner.0
    )
    .execute(ex)
    .await?;

    Ok(())
}

pub async fn add_image_url<'a, A>(
    conn: A,
    owner: Owner,
    proj: Project,
    img_name: &str,
    url: &str,
    content_type: &str,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    // insert row in images_revisions
    create_image_revision_row(
        &mut *tx,
        owner,
        proj,
        img_name,
        url,
        content_type,
        now
    ).await?;

    // update project to reflect the change
    update_project_non_project_data(&mut tx, owner, proj, now).await?;

    tx.commit().await?;

    Ok(())
}

pub async fn get_gallery<'e, E>(
    ex: E,
    proj: Project
) -> Result<Vec<GalleryImage>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_as!(
            GalleryImage,
            "
SELECT
    gallery_id AS id,
    filename,
    description
FROM galleries
WHERE project_id = ?
ORDER BY sort_key
            ",
            proj.0
        )
        .fetch_all(ex)
        .await?
    )
}

pub async fn get_gallery_at<'e, E>(
    ex: E,
    proj: Project,
    date: i64
) -> Result<Vec<GalleryImage>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_as!(
            GalleryImage,
            "
SELECT
    gallery_id AS id,
    filename,
    description
FROM galleries_history
WHERE project_id = ?
    AND published_at <= ?
    AND (removed_at > ? OR removed_at IS NULL)
ORDER BY sort_key
            ",
            proj.0,
            date,
            date
        )
        .fetch_all(ex)
        .await?
    )
}

async fn create_galleries_history_row<'e, E>(
    ex: E,
    owner: Owner,
    proj: Project,
    sort_key: &[u8],
    img_name: &str,
    description: &str,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT INTO galleries_history (
    project_id,
    sort_key,
    filename,
    description,
    published_at,
    published_by
)
VALUES (?, ?, ?, ?, ?, ?)
        ",
        proj.0,
        sort_key,
        img_name,
        description,
        now,
        owner.0
    )
    .execute(ex)
    .await
    .map_err(DatabaseError::from)
    .and_then(require_one_modified)
}

async fn retire_galleries_history_row<'e, E>(
    ex: E,
    owner: Owner,
    proj: Project,
    gallery_id: i64,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
UPDATE galleries_history
SET
    removed_by = ?,
    removed_at = ?
WHERE project_id = ?
    AND gallery_id = ?
        ",
        owner.0,
        now,
        proj.0,
        gallery_id,
    )
    .execute(ex)
    .await
    .map_err(DatabaseError::from)
    .and_then(require_one_modified)
}

async fn update_galleries_history_row_desc<'e, E>(
    ex: E,
    owner: Owner,
    gallery_id: i64,
    description: &str,
    now: i64
) -> Result<i64, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_scalar!(
        "
INSERT INTO galleries_history (
    project_id,
    sort_key,
    filename,
    description,
    published_at,
    published_by
)
SELECT
    project_id,
    sort_key,
    filename,
    ?,
    ?,
    ?
FROM galleries_history
WHERE gallery_id = ?
    AND removed_at = ?
RETURNING gallery_id
        ",
        description,
        now,
        owner.0,
        gallery_id,
        now
    )
    .fetch_one(ex)
    .await
    .map_err(DatabaseError::from)
}

async fn update_galleries_history_row_sort_key<'e, E>(
    ex: E,
    owner: Owner,
    gallery_id: i64,
    sort_key: &[u8],
    now: i64
) -> Result<i64, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_scalar!(
        "
INSERT INTO galleries_history (
    project_id,
    sort_key,
    filename,
    description,
    published_at,
    published_by
)
SELECT
    project_id,
    ?,
    filename,
    description,
    ?,
    ?
FROM galleries_history
WHERE gallery_id = ?
    AND removed_at = ?
RETURNING gallery_id
        ",
        sort_key,
        now,
        owner.0,
        gallery_id,
        now
    )
    .fetch_one(ex)
    .await
    .map_err(DatabaseError::from)
}

async fn get_last_key<'e, E>(
    ex: E,
    proj: Project
) -> Result<Option<Vec<u8>>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT MAX(sort_key)
FROM galleries_history
WHERE project_id = ?
    AND removed_at IS NULL
            ",
            proj.0
        )
        .fetch_one(ex)
        .await?
    )
}

async fn get_prev_next_keys<'e, E>(
    ex: E,
    proj: Project,
    next_id: Option<i64>
) -> Result<(Vec<u8>, Vec<u8>), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    match next_id {
        // find the sort keys for prev and next items
        Some(next_id) => {
            let r = sqlx::query!(
                "
SELECT
    prev_key,
    next_key
FROM (
    SELECT
        gallery_id,
        sort_key AS next_key,
        LAG(sort_key) OVER (ORDER BY sort_key) prev_key
    FROM galleries_history
    WHERE project_id = ?
        AND removed_at IS NULL
)
WHERE gallery_id = ?
                ",
                proj.0,
                next_id
            )
            .fetch_optional(ex)
            .await?
            .ok_or(DatabaseError::NotFound)?;

            match r.prev_key {
                Some(prev_key) => {
                    Ok((prev_key, r.next_key))
                },
                None => {
                    // the next item is first
                    Ok((vec![0x00], r.next_key))
                }
            }
        },
        None => {
            // there is no next item, so prev is the last item
            // find the sort key for the last item
            let prev_key = get_last_key(ex, proj)
                .await?
                .unwrap_or_else(|| vec![0x00]);

            let prev_key_len = prev_key.len();
            Ok((prev_key, vec![0xFF; prev_key_len + 1]))
        }
    }
}

async fn update_gallery_item(
    tx: &mut Transaction<'_, Sqlite>,
    owner: Owner,
    proj: Project,
    gallery_id: i64,
    description: &str,
    now: i64
) -> Result<i64, DatabaseError>
{
    retire_galleries_history_row(
        &mut **tx,
        owner,
        proj,
        gallery_id,
        now
    ).await?;

    update_galleries_history_row_desc(
        &mut **tx,
        owner,
        gallery_id,
        description,
        now
    ).await
}

async fn delete_gallery_item(
    tx: &mut Transaction<'_, Sqlite>,
    owner: Owner,
    proj: Project,
    gallery_id: i64,
    now: i64
) -> Result<(), DatabaseError>
{
    retire_galleries_history_row(
        &mut **tx,
        owner,
        proj,
        gallery_id,
        now
    ).await
}

async fn move_gallery_item(
    tx: &mut Transaction<'_, Sqlite>,
    owner: Owner,
    proj: Project,
    gallery_id: i64,
    next_id: Option<i64>,
    now: i64
) -> Result<i64, DatabaseError>
{
    retire_galleries_history_row(
        &mut **tx,
        owner,
        proj,
        gallery_id,
        now
    ).await?;

    // get sort keys of the prev and next items
    let (prev_key, next_key) = get_prev_next_keys(
        &mut **tx,
        proj,
        next_id
    ).await?;

    // find the midpoint between prev and next
    let sort_key = midpoint(&prev_key, &next_key);

    // insert the new row
    update_galleries_history_row_sort_key(
        &mut **tx,
        owner,
        gallery_id,
        &sort_key,
        now
    ).await
}

fn trailing(a: &[u8]) -> Vec<u8> {
    if a.iter().all(|b| *b == 0xFF) {
        let mut bv = vec![0xFF; a.len() + 1];
        bv[a.len()] = 0x01;
        bv
    }
    else {
        let bi: BigUint = BigUint::from_bytes_be(a) + 1u32;
        bi.to_bytes_be()
    }
}

fn midpoint(a: &[u8], b: &[u8]) -> Vec<u8> {
    // swap a, b if b is lesser
    let (a, b) = if a < b { (a, b) } else { (b, a) };

    // ensure that a, b have the same length
    let len = std::cmp::max(a.len(), b.len());

    let mut av = Vec::with_capacity(len);
    av.extend(a);
    av.resize(len, 0);

    let bv = if b.len() < len {
        let mut bv = Vec::with_capacity(len);
        bv.extend(b);
        bv.resize(len, 0);
        Some(bv)
    }
    else {
        None
    };

    let a = &av[..];
    let b = if let Some(ref bv) = bv { &bv[..] } else { b };

    // make a and b arbitrary-width integers
    let ai = BigUint::from_bytes_be(a);
    let bi = BigUint::from_bytes_be(b);

    // extend if a and b differ by one, otherwise take the average
    if &ai + 1u32 == bi {
        av.push(0x80);
        av
    }
    else {
        let mi: BigUint = (ai + bi) >> 1;
        mi.to_bytes_be()
    }
}

pub async fn update_gallery<'a, A>(
    conn: A,
    owner: Owner,
    proj: Project,
    gallery_patch: &GalleryPatch,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    let mut old_to_new = HashMap::new();

    for op in &gallery_patch.ops {
        match op {
            GalleryOp::Update { id, description } => {
                let cur = *old_to_new.get(id).unwrap_or(id);
                old_to_new.insert(
                    id,
                    update_gallery_item(
                        &mut tx, owner, proj, cur, description, now
                    ).await?
                );
            },
            GalleryOp::Delete { id } => {
                let cur = *old_to_new.get(id).unwrap_or(id);
                delete_gallery_item(
                    &mut tx, owner, proj, cur, now
                ).await?;
            },
            GalleryOp::Move { id, next } => {
                let cur = *old_to_new.get(id).unwrap_or(id);
                let next = next.as_ref()
                    .map(|n| *old_to_new.get(n).unwrap_or(n));
                old_to_new.insert(
                    id,
                    move_gallery_item(
                        &mut tx, owner, proj, cur, next, now
                    ).await?
                );
            }
        }
    }

    // update project to reflect the change
    update_project_non_project_data(&mut tx, owner, proj, now).await?;

    tx.commit().await?;

    Ok(())
}

pub async fn add_gallery_image<'a, A>(
    conn: A,
    owner: Owner,
    proj: Project,
    img_name: &str,
    url: &str,
    content_type: &str,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    // add row to images_revisions
    create_image_revision_row(
        &mut *tx,
        owner,
        proj,
        img_name,
        url,
        content_type,
        now
    ).await?;

    // find the sort key of the last item
    let last_key = get_last_key(&mut *tx, proj).await?;

    // generate a sort key following it
    let sort_key = match last_key {
        Some(last_key) if last_key.is_empty() =>
            // should not happen, violates db constraint
            return Err(DatabaseError::InvalidSortKey),
        Some(last_key) => trailing(&last_key),
        None => vec![0x40]
    };

    // add row to galleries_history
    create_galleries_history_row(
        &mut *tx,
        owner,
        proj,
        &sort_key,
        img_name,
        "",
        now
    ).await?;

    // update project to reflect the change
    update_project_non_project_data(&mut tx, owner, proj, now).await?;

    tx.commit().await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_url_ok(pool: Pool) {
        assert_eq!(
            get_image_url(&pool, Project(42), "img.png").await.unwrap(),
            Some("https://example.com/images/img.png".into())
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_url_not_a_project(pool: Pool) {
        assert_eq!(
            get_image_url(&pool, Project(1), "img.png").await.unwrap(),
            None
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_url_not_an_image(pool: Pool) {
        assert_eq!(
            get_image_url(&pool, Project(42), "bogus").await.unwrap(),
            None
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_url_at_ok(pool: Pool) {
        assert_eq!(
            get_image_url_at(
                &pool,
                Project(42),
                "img.png",
                1712012874000000000
            ).await.unwrap(),
            Some("https://example.com/images/img.png".into())
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_url_at_not_a_project(pool: Pool) {
        assert_eq!(
            get_image_url_at(&pool, Project(1), "img.png", 0).await.unwrap(),
            None
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_url_at_not_an_image(pool: Pool) {
        assert_eq!(
            get_image_url_at(&pool, Project(42), "bogus", 0).await.unwrap(),
            None
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn add_image_url_ok(pool: Pool) {
        assert_eq!(
            get_image_url(&pool, Project(42), "image.png").await.unwrap(),
            None
        );

        add_image_url(
            &pool,
            Owner(1),
            Project(42),
            "image.png",
            "https://example.com/image.png",
            "image/png",
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_image_url(&pool, Project(42), "image.png").await.unwrap(),
            Some("https://example.com/image.png".into())
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn add_image_url_not_a_user(pool: Pool) {
        // This should not happen; the Owner passed in should be good.
        assert!(
            matches!(
                add_image_url(
                    &pool,
                    Owner(0),
                    Project(42),
                    "image.png",
                    "https://example.com/image.png",
                    "image/png",
                    0
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn add_image_url_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert!(
            matches!(
                add_image_url(
                    &pool,
                    Owner(1),
                    Project(0),
                    "image.png",
                    "https://example.com/image.png",
                    "image/png",
                    0
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[test]
    fn midpoint_00_ff() {
        assert_eq!(midpoint(&[0x00], &[0xFF]), &[0x7F]);
    }

    #[test]
    fn midpoint_00_fe() {
        assert_eq!(midpoint(&[0x00], &[0xFE]), &[0x7F]);
    }

    #[test]
    fn midpoint_00_01() {
        assert_eq!(midpoint(&[0x00], &[0x01]), &[0x00, 0x80]);
    }

    #[test]
    fn midpoint_00ff_01() {
        assert_eq!(midpoint(&[0x00, 0xFF], &[0x01]), &[0x00, 0xFF, 0x80]);
    }

    #[test]
    fn trailling_00() {
        assert_eq!(trailing(&[0x00]), &[0x01]);
    }

    #[test]
    fn trailling_7f() {
        assert_eq!(trailing(&[0x7F]), &[0x80]);
    }

    #[test]
    fn trailling_ff_03() {
        assert_eq!(trailing(&[0xFF, 0x03]), &[0xFF, 0x04]);
    }

    #[test]
    fn trailling_ff() {
        assert_eq!(trailing(&[0xFF]), &[0xFF, 0x01]);
    }

    #[test]
    fn trailling_ffff() {
        assert_eq!(trailing(&[0xFF, 0xFF]), &[0xFF, 0xFF, 0x01]);
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_delete_ok(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(42)).await.unwrap(),
            [
                GalleryImage {
                    id: 1,
                    filename: "img.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(42),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Delete { id: 1 }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(42)).await.unwrap(),
            []
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_delete_nonexistent_id(pool: Pool) {
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(42),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Delete { id: 0 }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_delete_id_not_for_project(pool: Pool) {
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(6),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Delete { id: 1 }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_delete_id_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(0),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Delete { id: 1 }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_update(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(42)).await.unwrap(),
            [
                GalleryImage {
                    id: 1,
                    filename: "img.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(42),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Update { id: 1, description: "x".into() }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(42)).await.unwrap(),
            [
                GalleryImage {
                    id: 5,
                    filename: "img.png".into(),
                    description: "x".into()
                }
            ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_update_nonexistent_id(pool: Pool) {
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(42),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Update { id: 0, description: "x".into() }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_update_id_not_for_project(pool: Pool) {
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(6),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Update { id: 1, description: "x".into() }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_update_id_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(0),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Update { id: 1, description: "x".into() }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_end(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 3,
                    filename: "b.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 4,
                    filename: "c.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(6),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Move { id: 3,  next: None }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 4,
                    filename: "c.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 5,
                    filename: "b.png".into(),
                    description: "".into()
                },

            ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_start(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 3,
                    filename: "b.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 4,
                    filename: "c.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(6),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Move { id: 3,  next: Some(2) }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 5,
                    filename: "b.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 4,
                    filename: "c.png".into(),
                    description: "".into()
                }
            ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_mid(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 3,
                    filename: "b.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 4,
                    filename: "c.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(6),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Move { id: 4,  next: Some(3) }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 5,
                    filename: "c.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 3,
                    filename: "b.png".into(),
                    description: "".into()
                }
            ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_nonexistent_id(pool: Pool) {
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(6),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Move { id: 0,  next: None }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_nonexistent_next(pool: Pool) {
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(6),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Move { id: 2,  next: Some(0) }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_only_item(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(42)).await.unwrap(),
            [
                GalleryImage {
                    id: 1,
                    filename: "img.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(42),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Move { id: 1,  next: None }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(42)).await.unwrap(),
            [
                GalleryImage {
                    id: 5,
                    filename: "img.png".into(),
                    description: "".into()
                }
            ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_id_not_for_project(pool: Pool) {
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(42),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Move { id: 2,  next: None }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert_eq!(
            update_gallery(
                &pool,
                Owner(1),
                Project(0),
                &GalleryPatch {
                    ops: vec![
                        GalleryOp::Move { id: 2,  next: None }
                    ]
                },
                1703980420641538067
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_move(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 3,
                    filename: "b.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 4,
                    filename: "c.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(6),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Move { id: 3,  next: Some(2) },
                    GalleryOp::Move { id: 4,  next: Some(3) }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 6,
                    filename: "c.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 5,
                    filename: "b.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                }
            ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images", "galleries"))]
    async fn update_gallery_move_same_twice(pool: Pool) {
        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 3,
                    filename: "b.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 4,
                    filename: "c.png".into(),
                    description: "".into()
                }
            ]
        );

        update_gallery(
            &pool,
            Owner(1),
            Project(6),
            &GalleryPatch {
                ops: vec![
                    GalleryOp::Move { id: 4,  next: Some(3) },
                    GalleryOp::Move { id: 4,  next: Some(2) },
                    GalleryOp::Update { id: 4,  description: "x".into() }
                ]
            },
            1703980420641538067
        ).await.unwrap();

        assert_eq!(
            get_gallery(&pool, Project(6)).await.unwrap(),
            [
                GalleryImage {
                    id: 7,
                    filename: "c.png".into(),
                    description: "x".into()
                },
                GalleryImage {
                    id: 2,
                    filename: "a.png".into(),
                    description: "".into()
                },
                GalleryImage {
                    id: 3,
                    filename: "b.png".into(),
                    description: "".into()
                }
            ]
        );
    }
}
