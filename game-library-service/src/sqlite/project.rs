use itertools::Itertools;
use sqlx::{
    Acquire, Executor, QueryBuilder, Transaction,
    sqlite::Sqlite
};
use unicode_normalization::UnicodeNormalization;
use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

use crate::{
    db::{DatabaseError, ProjectRow, map_unique},
    input::{ProjectDataPatch, ProjectDataPost},
    model::{Owner, Project, User},
    sqlite::{
        require_one_modified,
        tags::update_project_tags_in_trans,
        users::add_owner
    }
};

pub async fn get_project_id<'e, E>(
    ex: E,
    projslug: &str
) -> Result<Option<Project>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
SELECT project_id
FROM projects
WHERE slug = ?
            ",
            projslug
        )
        .fetch_optional(ex)
        .await?
        .map(Project)
    )
}

fn normalize_project_name(proj: &str) -> String {
    // Requiring the normalized project name to be unique ensures that
    // project names are unique modulo case, marks, punctuation,
    // whitespace.

    // normalize and decompose
    proj.nfkd()
        // lowercase everything
        .flat_map(char::to_lowercase)
        // remove marks; map other non-alphanumerics to a space
        .filter_map(|c| match c.general_category_group() {
            GeneralCategoryGroup::Letter |
            GeneralCategoryGroup::Number => Some(c),
            GeneralCategoryGroup::Mark => None,
            _ => Some(' ')
        })
        // collapse runs of spaces
        .coalesce(|a, b|
            if a == ' ' && b == ' ' { Ok(' ') } else { Err((a, b)) }
        )
        .collect()
}

async fn create_project_history_row<'e, E>(
    ex: E,
    now: i64
) -> Result<Project, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
INSERT INTO projects_history (
    created_at
)
VALUES (?)
RETURNING project_id
            ",
            now
        )
        .fetch_one(ex)
        .await
        .map(Project)?
    )
}

async fn create_project_row<'e, E>(
    ex: E,
    user: User,
    proj: Project,
    slug: &str,
    proj_data: &ProjectDataPost,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    let proj_norm = normalize_project_name(&proj_data.name);

    sqlx::query!(
        "
INSERT INTO projects (
    project_id,
    name,
    normalized_name,
    slug,
    created_at,
    description,
    game_title,
    game_title_sort,
    game_publisher,
    game_year,
    game_players_min,
    game_players_max,
    game_length_min,
    game_length_max,
    readme,
    image,
    modified_at,
    modified_by,
    revision
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
        proj.0,
        proj_data.name,
        proj_norm,
        slug,
        now,
        proj_data.description,
        proj_data.game.title,
        proj_data.game.title_sort_key,
        proj_data.game.publisher,
        proj_data.game.year,
        proj_data.game.players.min,
        proj_data.game.players.max,
        proj_data.game.length.min,
        proj_data.game.length.max,
        "",
        None::<&str>,
        now,
        user.0,
        1
    )
    .execute(ex)
    .await
    .map_err(map_unique)?;

    Ok(())
}

#[derive(Debug)]
struct ProjectDataRow<'a> {
    project_id: i64,
    name: &'a str,
    slug: &'a str,
    description: &'a str,
    game_title: &'a str,
    game_title_sort: &'a str,
    game_publisher: &'a str,
    game_year: &'a str,
    game_players_min: Option<u32>,
    game_players_max: Option<u32>,
    game_length_min: Option<u32>,
    game_length_max: Option<u32>,
    readme: &'a str,
    image: Option<&'a str>
}

async fn create_project_data_row<'e, E>(
    ex: E,
    row: &ProjectDataRow<'_>
) -> Result<i64, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        sqlx::query_scalar!(
            "
INSERT INTO projects_data (
    project_id,
    name,
    slug,
    description,
    game_title,
    game_title_sort,
    game_publisher,
    game_year,
    game_players_min,
    game_players_max,
    game_length_min,
    game_length_max,
    readme,
    image
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING project_data_id
            ",
            row.project_id,
            row.name,
            row.slug,
            row.description,
            row.game_title,
            row.game_title_sort,
            row.game_publisher,
            row.game_year,
            row.game_players_min,
            row.game_players_max,
            row.game_length_min,
            row.game_length_max,
            row.readme,
            row.image
        )
        .fetch_one(ex)
        .await?
    )
}

#[derive(Debug)]
struct ProjectRevisionRow {
    project_id: i64,
    modified_at: i64,
    modified_by: i64,
    revision: i64,
    project_data_id: i64
}

async fn create_project_revision_row<'e, E>(
    ex: E,
    row: &ProjectRevisionRow
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query!(
        "
INSERT INTO projects_revisions (
    project_id,
    modified_at,
    modified_by,
    revision,
    project_data_id
)
VALUES (?, ?, ?, ?, ?)
        ",
        row.project_id,
        row.modified_at,
        row.modified_by,
        row.revision,
        row.project_data_id
    )
    .execute(ex)
    .await?;

    Ok(())
}

pub async fn create_project<'a, A>(
    conn: A,
    owner: User,
    slug: &str,
    pd: &ProjectDataPost,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    // create project history row
    let proj = create_project_history_row(&mut *tx, now).await?;

    // create project data
    let dr = ProjectDataRow {
        project_id: proj.0,
        name: &pd.name,
        slug,
        description: &pd.description,
        game_title: &pd.game.title,
        game_title_sort: &pd.game.title_sort_key,
        game_publisher:  &pd.game.publisher,
        game_year: &pd.game.year,
        game_players_min: pd.game.players.min,
        game_players_max: pd.game.players.max,
        game_length_min: pd.game.players.min,
        game_length_max: pd.game.players.max,
        readme: &pd.readme,
        image: pd.image.as_deref()
    };

    let project_data_id = create_project_data_row(&mut *tx, &dr).await?;

    // create project revision
    let rr = ProjectRevisionRow {
        project_id: proj.0,
        modified_at: now,
        modified_by: owner.0,
        revision: 1,
        project_data_id
    };

    create_project_revision_row(&mut *tx, &rr).await?;

    // create project
    create_project_row(&mut *tx, owner, proj, slug, pd, now).await?;

    // associate new owner with the project
    add_owner(&mut *tx, owner, proj).await?;

    // add tags
    if !pd.tags.is_empty() {
        update_project_tags_in_trans(
            &mut tx,
            Owner(owner.0),
            proj,
            &pd.tags,
            &[],
            now
        ).await?;
    }

    tx.commit().await?;

    Ok(())
}

async fn update_project_row<'e, E>(
    ex: E,
    owner: Owner,
    proj: Project,
    revision: i64,
    pd: &ProjectDataPatch,
    now: i64
) -> Result<(), DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    let mut qb: QueryBuilder<E::Database> = QueryBuilder::new(
        "UPDATE projects SET "
    );

    let mut qbs = qb.separated(", ");

    qbs
        .push("revision = ")
        .push_bind_unseparated(revision)
        .push("modified_at = ")
        .push_bind_unseparated(now)
        .push("modified_by = ")
        .push_bind_unseparated(owner.0);

    if let Some(description) = &pd.description {
        qbs.push("description = ").push_bind_unseparated(description);
    }

    if let Some(game_title) = &pd.game.title {
        qbs.push("game_title = ").push_bind_unseparated(game_title);
    }

    if let Some(game_title_sort) = &pd.game.title_sort_key {
        qbs.push("game_title_sort = ").push_bind_unseparated(game_title_sort);
    }

    if let Some(game_publisher) = &pd.game.publisher {
        qbs.push("game_publisher = ").push_bind_unseparated(game_publisher);
    }

    if let Some(game_year) = &pd.game.year {
        qbs.push("game_year = ").push_bind_unseparated(game_year);
    }

    if let Some(pmin) = &pd.game.players.min {
        qbs.push("game_players_min = ").push_bind_unseparated(pmin);
    }

    if let Some(pmax) = &pd.game.players.max {
        qbs.push("game_players_max = ").push_bind_unseparated(pmax);
    }

    if let Some(lmin) = &pd.game.length.min {
        qbs.push("game_length_min = ").push_bind_unseparated(lmin);
    }

    if let Some(lmax) = &pd.game.length.max {
        qbs.push("game_length_max = ").push_bind_unseparated(lmax);
    }

    if let Some(readme) = &pd.readme {
        qbs.push("readme = ").push_bind_unseparated(readme);
    }

    if let Some(image) = &pd.image {
        qbs.push("image = ").push_bind_unseparated(image);
    }

    qb
        .push(" WHERE project_id = ")
        .push_bind(proj.0)
        .build()
        .execute(ex)
        .await
        .map_err(DatabaseError::from)
        .and_then(require_one_modified)
}

pub async fn update_project<'a, A>(
    conn: A,
    owner: Owner,
    proj: Project,
    pd: &ProjectDataPatch,
    now: i64
) -> Result<(), DatabaseError>
where
    A: Acquire<'a, Database = Sqlite>
{
    let mut tx = conn.begin().await?;

    // get project
    let row = get_project_row(&mut *tx, proj).await?;
    let revision = row.revision + 1;

    // create project data
    let dr = ProjectDataRow {
        project_id: proj.0,
        name: &row.name,
        slug: &row.slug,
        description: pd.description.as_ref().unwrap_or(&row.description),
        game_title: pd.game.title.as_ref().unwrap_or(&row.game_title),
        game_title_sort: pd.game.title_sort_key.as_ref()
            .unwrap_or(&row.game_title_sort),
        game_publisher: pd.game.publisher.as_ref()
            .unwrap_or(&row.game_publisher),
        game_year: pd.game.year.as_ref().unwrap_or(&row.game_year),
        game_players_min: pd.game.players.min
            .unwrap_or(row.game_players_min
            .map(|p| p as u32)),
        game_players_max: pd.game.players.max
            .unwrap_or(row.game_players_max
            .map(|p| p as u32)),
        game_length_min: pd.game.length.min
            .unwrap_or(row.game_length_min
            .map(|l| l as u32)),
        game_length_max: pd.game.length.max
            .unwrap_or(row.game_length_max
            .map(|l| l as u32)),
        readme: pd.readme.as_ref().unwrap_or(&row.readme),
        image: pd.image.as_ref().unwrap_or(&row.image).as_deref()
    };

    let project_data_id = create_project_data_row(&mut *tx, &dr).await?;

    // update tags if required
    if pd.tags_add.is_some() || pd.tags_remove.is_some() {
        update_project_tags_in_trans(
            &mut tx,
            owner,
            proj,
            pd.tags_add.as_ref().map_or(&[], |v| v),
            pd.tags_remove.as_ref().map_or(&[], |v| v),
            now
        ).await?;
    }

    let rr = ProjectRevisionRow {
        project_id: proj.0,
        modified_at: now,
        modified_by: owner.0,
        revision,
        project_data_id
    };

    // create project revision
    create_project_revision_row(&mut *tx, &rr).await?;

    // update project
    update_project_row(&mut *tx, owner, proj, revision, pd, now).await?;

    tx.commit().await?;

    Ok(())
}

pub async fn get_project_row<'e, E>(
    ex: E,
    proj: Project
) -> Result<ProjectRow, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_as!(
        ProjectRow,
        "
SELECT
    project_id,
    name,
    slug,
    description,
    revision,
    created_at,
    modified_at,
    modified_by,
    game_title,
    game_title_sort,
    game_publisher,
    game_year,
    game_players_min,
    game_players_max,
    game_length_min,
    game_length_max,
    readme,
    image
FROM projects
WHERE project_id = ?
LIMIT 1
        ",
        proj.0
    )
    .fetch_optional(ex)
    .await?
    .ok_or(DatabaseError::NotFound)
}

pub async fn get_project_row_revision<'e, E>(
    ex: E,
    proj: Project,
    revision: i64
) -> Result<ProjectRow, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_as!(
        ProjectRow,
        "
SELECT
    projects_revisions.project_id,
    projects_data.name,
    projects_data.slug,
    projects_data.description,
    projects_revisions.revision,
    projects_history.created_at,
    projects_revisions.modified_at,
    projects_revisions.modified_by,
    projects_data.game_title,
    projects_data.game_title_sort,
    projects_data.game_publisher,
    projects_data.game_year,
    projects_data.game_players_min,
    projects_data.game_players_max,
    projects_data.game_length_min,
    projects_data.game_length_max,
    projects_data.image,
    projects_data.readme
FROM projects_revisions
JOIN projects_data
ON projects_revisions.project_data_id = projects_data.project_data_id
JOIN projects_history
ON projects_revisions.project_id = projects_history.project_id
WHERE projects_revisions.project_id = ?
    AND projects_revisions.revision = ?
LIMIT 1
        ",
        proj.0,
        revision
    )
    .fetch_optional(ex)
    .await?
    .ok_or(DatabaseError::NotFound)
}

async fn get_project_data_id<'e, E>(
    ex: E,
    proj: Project,
    revision: i64
) -> Result<i64, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    sqlx::query_scalar!(
        "
SELECT
    projects_revisions.project_data_id
FROM projects_revisions
WHERE projects_revisions.project_id = ?
    AND projects_revisions.revision = ?
LIMIT 1
        ",
        proj.0,
        revision
    )
    .fetch_optional(ex)
    .await?
    .ok_or(DatabaseError::NotFound)
}

pub async fn update_project_non_project_data(
    tx: &mut Transaction<'_, Sqlite>,
    owner: Owner,
    proj: Project,
    now: i64,
) -> Result<(), DatabaseError>
{
    // get the project row, project_data_id
    let row = get_project_row(&mut **tx, proj).await?;

    let project_data_id = get_project_data_id(&mut **tx, proj, row.revision)
        .await?;

    let revision = row.revision + 1;

    // insert a new project revision row
    let rr = ProjectRevisionRow {
        project_id: proj.0,
        modified_at: now,
        modified_by: owner.0,
        revision,
        project_data_id
    };

    create_project_revision_row(&mut **tx, &rr).await?;

    // update the project row
    update_project_row(
        &mut **tx,
        owner,
        proj,
        revision,
        &Default::default(),
        now
    ).await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use once_cell::sync::Lazy;

    use crate::{
        input::{GameDataPost, RangePost},
        sqlite::tags::get_project_tags
    };

    type Pool = sqlx::Pool<Sqlite>;

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_project_id_ok(pool: Pool) {
        assert_eq!(
            get_project_id(&pool, "test_game").await.unwrap(),
            Some(Project(42))
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_project_id_not_a_project(pool: Pool) {
        assert_eq!(
            get_project_id(&pool, "bogus").await.unwrap(),
            None
        );
    }

    #[test]
    fn normalize_project_names() {
        assert_eq!(normalize_project_name("foo"), "foo");
        assert_eq!(normalize_project_name("FoO"), "foo");
        assert_eq!(normalize_project_name("foo_bar"), "foo bar");
        assert_eq!(normalize_project_name("foo-BAR"), "foo bar");
        assert_eq!(normalize_project_name("foo  bar"), "foo bar");
        assert_eq!(normalize_project_name("fÖÖ bar"), "foo bar");
    }

    static CREATE_ROW: Lazy<ProjectRow> = Lazy::new(||
        ProjectRow {
            project_id: 1,
            name: "test_game".into(),
            slug: "test_game".into(),
            description: "Brian's Trademarked Game of Being a Test Case".into(),
            revision: 1,
            created_at: 1699804206419538067,
            modified_at: 1699804206419538067,
            modified_by: 1,
            game_title: "A Game of Tests".into(),
            game_title_sort: "Game of Tests, A".into(),
            game_publisher: "Test Game Company".into(),
            game_year: "1979".into(),
            game_players_min: None,
            game_players_max: Some(3),
            game_length_min: None,
            game_length_max: None,
            readme: "".into(),
            image: None
        }
    );

    static CREATE_DATA: Lazy<ProjectDataPost> = Lazy::new(||
        ProjectDataPost {
            name: CREATE_ROW.name.clone(),
            description: CREATE_ROW.description.clone(),
            tags: vec!["a".into()],
            game: GameDataPost {
                title: CREATE_ROW.game_title.clone(),
                title_sort_key: CREATE_ROW.game_title_sort.clone(),
                publisher: CREATE_ROW.game_publisher.clone(),
                year: CREATE_ROW.game_year.clone(),
                players: RangePost {
                    min: CREATE_ROW.game_players_min.map(|i| i as u32),
                    max: CREATE_ROW.game_players_max.map(|i| i as u32)
                },
                length: RangePost {
                    min: CREATE_ROW.game_length_min.map(|i| i as u32),
                    max: CREATE_ROW.game_length_max.map(|i| i as u32)
                }
            },
            readme: "".into(),
            image: None
        }
    );

    #[sqlx::test(fixtures("users"))]
    async fn create_project_ok(pool: Pool) {
        assert_eq!(
            get_project_id(&pool, &CREATE_ROW.name).await.unwrap(),
            None
        );

        create_project(
            &pool,
            User(1),
            &CREATE_ROW.name,
            &CREATE_DATA,
            CREATE_ROW.created_at
        ).await.unwrap();

        let proj = get_project_id(&pool, &CREATE_ROW.name)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            get_project_row(&pool, proj).await.unwrap(),
            *CREATE_ROW
        );

         assert_eq!(
            get_project_tags(&pool, proj).await.unwrap(),
            ["a".to_string()]
        );
    }

    #[sqlx::test(fixtures("users"))]
    async fn create_project_not_a_user(pool: Pool) {
        assert_eq!(
            get_project_id(&pool, &CREATE_ROW.name).await.unwrap(),
            None
        );

        assert!(
            matches!(
                create_project(
                    &pool,
                    User(0),
                    &CREATE_ROW.name,
                    &CREATE_DATA,
                    CREATE_ROW.created_at
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );

        assert_eq!(
            get_project_id(&pool, &CREATE_ROW.name).await.unwrap(),
            None
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn create_project_already_exists_exact_match(pool: Pool) {
        let row = ProjectRow {
            project_id: 42,
            ..CREATE_ROW.clone()
        };

        assert_eq!(
            get_project_id(&pool, &CREATE_ROW.name).await.unwrap(),
            Some(Project(row.project_id))
        );

        assert_eq!(
            create_project(
                &pool,
                User(1),
                &row.name,
                &CREATE_DATA,
                row.created_at
            ).await.unwrap_err(),
            DatabaseError::AlreadyExists
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn create_project_already_exists_no_case_match(pool: Pool) {
        let row = ProjectRow {
            project_id: 42,
            name: CREATE_ROW.name.to_uppercase(),
            ..CREATE_ROW.clone()
        };

        assert_eq!(
            get_project_id(&pool, &CREATE_ROW.name).await.unwrap(),
            Some(Project(row.project_id))
        );

        assert_eq!(
            create_project(
                &pool,
                User(1),
                &row.name,
                &CREATE_DATA,
                row.created_at
            ).await.unwrap_err(),
            DatabaseError::AlreadyExists
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn create_project_already_exists_hyphen_match(pool: Pool) {
        let row = ProjectRow {
            project_id: 42,
            name: CREATE_ROW.name.replace('_', "-"),
            ..CREATE_ROW.clone()
        };

        assert_eq!(
            get_project_id(&pool, &CREATE_ROW.name).await.unwrap(),
            Some(Project(row.project_id))
        );

        assert_eq!(
            create_project(
                &pool,
                User(1),
                &row.name,
                &CREATE_DATA,
                row.created_at
            ).await.unwrap_err(),
            DatabaseError::AlreadyExists
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn update_project_ok(pool: Pool) {
        let proj = Project(42);
        let orig_row = get_project_row(&pool, proj).await.unwrap();

        let pd = ProjectDataPatch {
            description: Some("foo".into()),
            ..Default::default()
        };

        update_project(
            &pool,
            Owner(1),
            proj,
            &pd,
            1702569006419538068
        ).await.unwrap();

        let new_row = get_project_row(&pool, proj).await.unwrap();

        assert_ne!(orig_row.description, pd.description.as_deref().unwrap());
        assert_eq!(new_row.description, pd.description.as_deref().unwrap());
        assert_eq!(new_row.revision, orig_row.revision + 1);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn update_project_not_a_project(pool: Pool) {
        let pd = ProjectDataPatch {
            description: Some("foo".into()),
            ..Default::default()
        };

        assert_eq!(
            update_project(
                &pool,
                Owner(1),
                Project(0),
                &pd,
                0
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn update_project_not_a_user(pool: Pool) {
        // This should not happen; the Owner passed in should be good.
        let pd = ProjectDataPatch {
            description: Some("foo".into()),
            ..Default::default()
        };

        assert!(
            matches!(
                update_project(
                    &pool,
                    Owner(0),
                    Project(42),
                    &pd,
                    0
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn update_project_non_project_data_ok(pool: Pool) {

        let proj = Project(42);
        let orig_row = get_project_row(&pool, proj).await.unwrap();

        let mut tx = pool.begin().await.unwrap();

        update_project_non_project_data(
            &mut tx,
            Owner(1),
            proj,
            1702569006419538068
        ).await.unwrap();

        tx.commit().await.unwrap();

        let new_row = get_project_row(&pool, proj).await.unwrap();

        assert_eq!(new_row.revision, orig_row.revision + 1);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn update_project_non_project_data_not_a_project(pool: Pool) {
        let mut tx = pool.begin().await.unwrap();

        // This should not happen; the Project passed in should be good.
        assert_eq!(
            update_project_non_project_data(
                &mut tx,
                Owner(1),
                Project(0),
                0
            ).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn update_project_non_project_data_not_a_user(pool: Pool) {
        let mut tx = pool.begin().await.unwrap();

        // This should not happen; the Owner passed in should be good.
        assert!(
            matches!(
                update_project_non_project_data(
                    &mut tx,
                    Owner(0),
                    Project(42),
                    0
                ).await.unwrap_err(),
                DatabaseError::SqlxError(_)
            )
        );
    }

    static CUR_ROW: Lazy<ProjectRow> = Lazy::new(||
        ProjectRow {
            project_id: 42,
            name: "test_game".into(),
            slug: "test_game".into(),
            description: "Brian's Trademarked Game of Being a Test Case".into(),
            revision: 3,
            created_at: 1699804206419538067,
            modified_at: 1702569006419538067,
            modified_by: 1,
            game_title: "A Game of Tests".into(),
            game_title_sort: "game of tests, a".into(),
            game_publisher: "Test Game Company".into(),
            game_year: "1979".into(),
            game_players_min: None,
            game_players_max: Some(3),
            game_length_min: None,
            game_length_max: None,
            readme: "".into(),
            image: None
        }
    );

    static OLD_ROW: Lazy<ProjectRow> = Lazy::new(||
        ProjectRow {
            project_id: 42,
            name: "test_game".into(),
            slug: "test_game".into(),
            description: "Brian's Trademarked Game of Being a Test Case".into(),
            revision: 1,
            created_at: 1699804206419538067,
            modified_at: 1699804206419538067,
            modified_by: 1,
            game_title: "A Game of Tests".into(),
            game_title_sort: "game of tests, a".into(),
            game_publisher: "Test Game Company".into(),
            game_year: "1978".into(),
            game_players_min: None,
            game_players_max: Some(3),
            game_length_min: None,
            game_length_max: None,
            readme: "".into(),
            image: None
        }
    );

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_project_row_ok(pool: Pool) {
        assert_eq!(
            get_project_row(&pool, Project(42)).await.unwrap(),
            *CUR_ROW
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_project_row_not_a_project(pool: Pool) {
        assert_eq!(
            get_project_row(&pool, Project(0)).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners"))]
    async fn get_project_row_revision_ok_current(pool: Pool) {
        assert_eq!(
            get_project_row_revision(&pool, Project(42), 3).await.unwrap(),
            *CUR_ROW
        );
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners"))]
    async fn get_project_revision_ok_old(pool: Pool) {
        assert_eq!(
            get_project_row_revision(&pool, Project(42), 1).await.unwrap(),
            *OLD_ROW
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_project_revision_not_a_project(pool: Pool) {
        // This should not happen; the Project passed in should be good.
        assert_eq!(
            get_project_row_revision(&pool, Project(0), 2).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_project_revision_not_a_revision(pool: Pool) {
        assert_eq!(
            get_project_row_revision(&pool, Project(42), 0).await.unwrap_err(),
            DatabaseError::NotFound
        );
    }
}
