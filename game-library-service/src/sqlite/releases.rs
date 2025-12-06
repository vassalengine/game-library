use sqlx::{
    Acquire, Executor,
    sqlite::Sqlite
};
use std::cmp::Ordering;

use crate::{
    db::{DatabaseError, FileRow, ReleaseRow, map_unique},
    model::{Owner, Package, Project, Release},
    sqlite::{
        require_one_modified,
        project::update_project_non_project_data
    },
    version::Version
};

impl From<&ReleaseRow> for Version {
    fn from(r: &ReleaseRow) -> Self {
        Version {
            major: r.version_major,
            minor: r.version_minor,
            patch: r.version_patch,
            pre: Some(&r.version_pre).filter(|v| !v.is_empty()).cloned(),
            build: Some(&r.version_build).filter(|v| !v.is_empty()).cloned()
        }
    }
}

struct ReleaseVersionRow {
    pub version_major: i64,
    pub version_minor: i64,
    pub version_patch: i64,
    pub version_pre: String,
    pub version_build: String
}

impl From<ReleaseVersionRow> for Version {
    fn from(r: ReleaseVersionRow) -> Self {
        Version {
            major: r.version_major,
            minor: r.version_minor,
            patch: r.version_patch,
            pre: Some(&r.version_pre).filter(|v| !v.is_empty()).cloned(),
            build: Some(&r.version_build).filter(|v| !v.is_empty()).cloned()
        }
    }
}

fn release_row_desc_cmp<R>(a: &R, b: &R) -> Ordering
where
    Version: for<'r> From<&'r R>
{
    let av: Version = a.into();
    let bv: Version = b.into();
    bv.cmp(&av)
}

pub async fn get_releases<'e, E>(
    ex: E,
    pkg: Package
) -> Result<Vec<ReleaseRow>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    let mut releases = sqlx::query_as!(
        ReleaseRow,
        "
SELECT
    releases.release_id,
    releases.version,
    releases.version_major,
    releases.version_minor,
    releases.version_patch,
    releases.version_pre,
    releases.version_build,
    releases.published_at,
    users.username AS published_by
FROM releases
JOIN users
ON releases.published_by = users.user_id
WHERE releases.package_id = ?
ORDER BY
    releases.version_major DESC,
    releases.version_minor DESC,
    releases.version_patch DESC,
    releases.version_pre ASC,
    releases.version_build ASC
        ",
        pkg.0
    )
    .fetch_all(ex)
    .await?;

    releases.sort_by(release_row_desc_cmp);
    Ok(releases)
}

pub async fn get_releases_at<'e, E>(
    ex: E,
    pkg: Package,
    date: i64
) -> Result<Vec<ReleaseRow>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    let mut releases = sqlx::query_as!(
        ReleaseRow,
        "
SELECT
    releases_history.release_id,
    releases_history.version,
    releases_history.version_major,
    releases_history.version_minor,
    releases_history.version_patch,
    releases_history.version_pre,
    releases_history.version_build,
    releases_history.published_at,
    users.username AS published_by
FROM releases_history
JOIN users
ON releases_history.published_by = users.user_id
WHERE releases_history.package_id = ?
    AND releases_history.published_at <= ?
    AND (
        ? < releases_history.deleted_at OR
        releases_history.deleted_at IS NULL
    )
ORDER BY
    releases_history.version_major DESC,
    releases_history.version_minor DESC,
    releases_history.version_patch DESC,
    releases_history.version_pre ASC,
    releases_history.version_build ASC
        ",
        pkg.0,
        date,
        date
    )
    .fetch_all(ex)
    .await?;

    releases.sort_by(release_row_desc_cmp);
    Ok(releases)
}

pub async fn get_release_id<'e, E>(
    ex: E,
    proj: Project,
    pkg: Package,
    release: &str
) -> Result<Option<Release>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT release_id
FROM releases
WHERE package_id = ?
    AND version = ?
            ",
            pkg.0,
            release
        )
        .fetch_optional(ex)
        .await?
        .map(Release)
    )
}

pub async fn get_project_package_release_ids<'e, E>(
    ex: E,
    projslug: &str,
    pkgslug: &str,
    release: &str
) -> Result<Option<(Project, Package, Release)>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query!(
            "
SELECT packages.project_id,
    packages.package_id,
    releases.release_id
FROM projects
JOIN packages
ON projects.project_id = packages.project_id
JOIN releases
ON packages.package_id = releases.package_id
WHERE projects.slug = ?
    AND packages.slug = ?
    AND releases.version = ?
            ",
            projslug,
            pkgslug,
            release
        )
        .fetch_optional(ex)
        .await?
        .map(|r| (
            Project(r.project_id),
            Package(r.package_id),
            Release(r.release_id)
        ))
    )
}

async fn create_release_history_row<'e, E>(
    ex: E,
    owner: Owner,
    pkg: Package,
    version: &Version,
    now: i64
) -> Result<Release, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    let vstr = String::from(version);
    let pre = version.pre.as_deref().unwrap_or("");
    let build = version.build.as_deref().unwrap_or("");

    sqlx::query_scalar!(
        "
INSERT INTO releases_history (
    package_id,
    version,
    version_major,
    version_minor,
    version_patch,
    version_pre,
    version_build,
    published_at,
    published_by
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING release_id
        ",
        pkg.0,
        vstr,
        version.major,
        version.minor,
        version.patch,
        pre,
        build,
        now,
        owner.0
    )
    .fetch_one(ex)
    .await
    .map(Release)
    .map_err(map_unique)
}

pub async fn create_release<'a, A>(
    conn: A,
    owner: Owner,
    proj: Project,
    pkg: Package,
    version: &Version,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    // insert release row
    let rel = create_release_history_row(
        &mut *tx,
        owner,
        pkg,
        version,
        now
    ).await?;

    // update project to reflect the change
    update_project_non_project_data(&mut tx, owner, proj, now).await?;

    tx.commit().await?;

    Ok(())
}

pub async fn get_release_version<'e, E>(
    ex: E,
    release: Release
) -> Result<Version, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_as!(
            ReleaseVersionRow,
            "
    SELECT
        version_major,
        version_minor,
        version_patch,
        version_pre,
        version_build
    FROM releases
    WHERE release_id = ?
            ",
            release.0
        )
        .fetch_one(ex)
        .await?
        .into()
    )
}

pub async fn get_files<'e, E>(
    ex: E,
    release: Release
) -> Result<Vec<FileRow>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_as!(
            FileRow,
            "
SELECT
    files.file_id AS id,
    files.url,
    files.filename,
    files.size,
    files.sha256,
    files.requires,
    files.published_at,
    users.username AS published_by
FROM files
JOIN users
ON files.published_by = users.user_id
WHERE files.release_id = ?
ORDER BY
    files.filename ASC
            ",
            release.0
        )
        .fetch_all(ex)
        .await?
    )
}

pub async fn get_files_at<'e, E>(
    ex: E,
    release: Release,
    date: i64
) -> Result<Vec<FileRow>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_as!(
            FileRow,
            "
SELECT
    files.file_id AS id,
    files.url,
    files.filename,
    files.size,
    files.sha256,
    files.requires,
    files.published_at,
    users.username AS published_by
FROM files
JOIN users
ON files.published_by = users.user_id
WHERE files.release_id = ?
    AND files.published_at <= ?
ORDER BY
    files.filename ASC
            ",
            release.0,
            date
        )
        .fetch_all(ex)
        .await?
    )
}

#[allow(clippy::too_many_arguments)]
async fn create_file_row<'e, E>(
    ex: E,
    owner: Owner,
    release: Release,
    filename: &str,
    size: i64,
    sha256: &str,
    content_type: &str,
    requires: Option<&str>,
    url: &str,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT INTO files (
    release_id,
    url,
    filename,
    size,
    sha256,
    content_type,
    requires,
    published_at,
    published_by
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
        release.0,
        url,
        filename,
        size,
        sha256,
        content_type,
        requires,
        now,
        owner.0
    )
    .execute(ex)
    .await
    .map_err(map_unique)?;

    Ok(())
}

async fn retire_release_history_row<'e, E>(
    ex: E,
    owner: Owner,
    rel: Release,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
UPDATE releases_history
SET
    deleted_by = ?,
    deleted_at = ?
WHERE release_id = ?
        ",
        owner.0,
        now,
        rel.0
    )
    .execute(ex)
    .await
    .map_err(DatabaseError::from)
    .and_then(require_one_modified)
}

pub async fn delete_release<'a, A>(
    conn: A,
    owner: Owner,
    proj: Project,
    rel: Release,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    // delete release row
    retire_release_history_row(&mut *tx, owner, rel, now).await?;

    // update project to reflect the change
    update_project_non_project_data(&mut tx, owner, proj, now).await?;

    tx.commit().await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn add_file_url<'a, A>(
    conn: A,
    owner: Owner,
    proj: Project,
    release: Release,
    filename: &str,
    size: i64,
    sha256: &str,
    content_type: &str,
    requires: Option<&str>,
    url: &str,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    // insert file row
    create_file_row(
        &mut *tx,
        owner,
        release,
        filename,
        size,
        sha256,
        content_type,
        requires,
        url,
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

    use std::{slice, sync::LazyLock};

    use crate::sqlite::project::get_project_row;

    type Pool = sqlx::Pool<Sqlite>;

    static RR_1_2_3: LazyLock<ReleaseRow> = LazyLock::new(||
        ReleaseRow {
            release_id: 1,
            version: "1.2.3".into(),
            version_major: 1,
            version_minor: 2,
            version_patch: 3,
            version_pre: "".into(),
            version_build: "".into(),
            published_at: 1702137389180282477,
            published_by: "bob".into()
        }
    );

/*
        FileRow {
            id: 1,
            url: "https://example.com/a_package-1.2.3".into(),
            filename: "a_package-1.2.3".into(),
            size: 1234,
            sha256: "c0e0fa7373a12b45a91e4f4d4e2e186442fc6ee9b346caa2fdc1c09026a2144a".into(),
            requires: ">= 3.2.17".into(),
            published_at: 1702137389180282477,
            published_by: "bob".into()
        }
*/

    static RR_1_2_4: LazyLock<ReleaseRow> = LazyLock::new(||
        ReleaseRow {
            release_id: 2,
            version: "1.2.4".into(),
            version_major: 1,
            version_minor: 2,
            version_patch: 4,
            version_pre: "".into(),
            version_build: "".into(),
            published_at: 1702223789180282477,
            published_by: "alice".into()
        }
    );

    static RR_1_2_5: LazyLock<ReleaseRow> = LazyLock::new(||
        ReleaseRow {
            release_id: 4,
            version: "1.2.5".into(),
            version_major: 1,
            version_minor: 2,
            version_patch: 5,
            version_pre: "".into(),
            version_build: "".into(),
            published_at: 1702223789180282477,
            published_by: "alice".into()
        }
    );

/*
        FileRow {
            id: 2,
            version: "1.2.4".into(),
            version_major: 1,
            version_minor: 2,
            version_patch: 4,
            version_pre: "".into(),
            version_build: "".into(),
            url: "https://example.com/a_package-1.2.4".into(),
            filename: "a_package-1.2.4".into(),
            size: 5678,
            sha256: "79fdd8fe3128f818e446e919cce5dcfb81815f8f4341c53f4d6b58ded48cebf2".into(),
            requires: ">= 3.7.12".into(),
            published_at: 1702223789180282477,
            published_by: "alice".into()
        }
*/

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn get_releases_ok(pool: Pool) {
        assert_eq!(
            get_releases(&pool, Package(1)).await.unwrap(),
            [ RR_1_2_5.clone(), RR_1_2_4.clone(), RR_1_2_3.clone() ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn get_releases_not_a_package(pool: Pool) {
        // This should not happen; the Package passed in should be good.
        // However, it's not an error if it does.
        assert_eq!(
            get_releases(&pool, Package(0)).await.unwrap(),
            []
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn get_releases_at_all(pool: Pool) {
        assert_eq!(
            get_releases_at(&pool, Package(1), 1705223789180282477)
                .await
                .unwrap(),
            [ RR_1_2_5.clone(), RR_1_2_4.clone(), RR_1_2_3.clone() ]
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn get_releases_at_some(pool: Pool) {
        assert_eq!(
            &get_releases_at(&pool, Package(1), 1702137399180282477)
                .await
                .unwrap(),
            slice::from_ref(&*RR_1_2_3)
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn get_releases_at_none(pool: Pool) {
        assert_eq!(
            get_releases_at(&pool, Package(1), 0).await.unwrap(),
            []
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn get_releases_at_not_a_package(pool: Pool) {
        // This should not happen; the Package passed in should be good.
        // However, it's not an error if it does.
        assert_eq!(
            get_releases_at(&pool, Package(0), 0).await.unwrap(),
            []
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn get_release_version_ok(pool: Pool) {
        assert_eq!(
            get_release_version(&pool, Release(1)).await.unwrap(),
            Version { major: 1, minor: 2, patch: 3, pre: None, build: None }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_release_ok(pool: Pool) {
        let proj = Project(42);
        let pkg = Package(2);

        assert_eq!(
            get_releases(&pool, pkg).await.unwrap(),
            []
        );

        assert_eq!(
            get_releases_at(&pool, pkg, 1699804206419538067).await.unwrap(),
            []
        );

        assert_eq!(
            get_project_row(&pool, proj).await.unwrap().revision,
            3
        );

        let ver = Version {
            major: 1,
            minor: 2,
            patch: 3,
            pre: None,
            build: None
        };

        create_release(
            &pool,
            Owner(1),
            proj,
            pkg,
            &ver,
            1699804206419538067
        ).await.unwrap();

        let rr = ReleaseRow {
            release_id: 5,
            version: "1.2.3".into(),
            version_major: 1,
            version_minor: 2,
            version_patch: 3,
            version_pre: "".into(),
            version_build: "".into(),
            published_at: 1699804206419538067,
            published_by: "bob".into()
        };

        assert_eq!(
            &get_releases(&pool, pkg).await.unwrap(),
            slice::from_ref(&rr)
        );

        assert_eq!(
            &get_releases_at(&pool, pkg, 1699804206419538067).await.unwrap(),
            slice::from_ref(&rr)
        );

        assert_eq!(
            get_project_row(&pool, proj).await.unwrap().revision,
            4
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_release_not_a_project(pool: Pool) {
        let ver = Version {
            major: 1,
            minor: 2,
            patch: 3,
            pre: None,
            build: None
        };

        assert!(
            matches!(
                create_release(
                    &pool,
                    Owner(1),
                    Project(0),
                    Package(6),
                    &ver,
                    1699804206419538067
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_release_not_a_package(pool: Pool) {
        let ver = Version {
            major: 1,
            minor: 2,
            patch: 3,
            pre: None,
            build: None
        };

        assert!(
            matches!(
                create_release(
                    &pool,
                    Owner(1),
                    Project(42),
                    Package(6),
                    &ver,
                    1699804206419538067
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_release_already_exists(pool: Pool) {
        let ver = Version {
            major: 1,
            minor: 2,
            patch: 3,
            pre: None,
            build: None
        };

        assert_eq!(
            create_release(
                &pool,
                Owner(1),
                Project(42),
                Package(1),
                &ver,
                1699804206419538067
            ).await.unwrap_err(),
            DatabaseError::AlreadyExists
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn delete_release_ok(pool: Pool) {
        let proj = Project(42);
        let pkg = Package(1);

        let rels_before = [
            ReleaseRow {
                release_id: 4,
                version: "1.2.5".into(),
                version_major: 1,
                version_minor: 2,
                version_patch: 5,
                version_pre: "".into(),
                version_build: "".into(),
                published_at: 1702223789180282477,
                published_by: "alice".into()
            },
            ReleaseRow {
                release_id: 2,
                version: "1.2.4".into(),
                version_major: 1,
                version_minor: 2,
                version_patch: 4,
                version_pre: "".into(),
                version_build: "".into(),
                published_at: 1702223789180282477,
                published_by: "alice".into()
            },
            ReleaseRow {
                release_id: 1,
                version: "1.2.3".into(),
                version_major: 1,
                version_minor: 2,
                version_patch: 3,
                version_pre: "".into(),
                version_build: "".into(),
                published_at: 1702137389180282477,
                published_by: "bob".into()
            }
        ];

        assert_eq!(
            get_releases(&pool, pkg).await.unwrap(),
            rels_before
        );

        assert_eq!(
            get_releases_at(&pool, pkg, 1702237389180282477).await.unwrap(),
            rels_before
        );

        assert_eq!(
            get_project_row(&pool, proj).await.unwrap().revision,
            3
        );

        delete_release(
            &pool,
            Owner(1),
            Project(42),
            Release(4),
            1702223789180282478
        ).await.unwrap();

        let rels_after = [
            ReleaseRow {
                release_id: 2,
                version: "1.2.4".into(),
                version_major: 1,
                version_minor: 2,
                version_patch: 4,
                version_pre: "".into(),
                version_build: "".into(),
                published_at: 1702223789180282477,
                published_by: "alice".into()
            },
            ReleaseRow {
                release_id: 1,
                version: "1.2.3".into(),
                version_major: 1,
                version_minor: 2,
                version_patch: 3,
                version_pre: "".into(),
                version_build: "".into(),
                published_at: 1702137389180282477,
                published_by: "bob".into()
            }
        ];

        assert_eq!(
            get_releases(&pool, pkg).await.unwrap(),
            rels_after
        );

        assert_eq!(
            get_releases_at(&pool, pkg, 1702223789180282477).await.unwrap(),
            rels_before
        );

        assert_eq!(
            get_releases_at(&pool, pkg, 1702223789180282478).await.unwrap(),
            rels_after
        );

        assert_eq!(
            get_project_row(&pool, proj).await.unwrap().revision,
            4
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn delete_release_not_project(pool: Pool) {
        assert!(
            matches!(
                delete_release(
                    &pool,
                    Owner(1),
                    Project(0),
                    Release(2),
                    1702137389180282478
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn delete_release_not_release(pool: Pool) {
        assert_eq!(
            delete_release(
                &pool,
                Owner(1),
                Project(42),
                Release(5),
                1702137389180282478
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn delete_release_not_empty(pool: Pool) {
        assert!(
            matches!(
                delete_release(
                    &pool,
                    Owner(1),
                    Project(42),
                    Release(1),
                    1702137389180282478
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

// TODO: add_file_url tests
}
