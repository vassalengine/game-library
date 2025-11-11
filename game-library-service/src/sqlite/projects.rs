use const_format::formatcp;
use glc::pagination::{Direction, Facet, SortBy};
use sqlx::{
    Encode, Executor, QueryBuilder, Type,
    query_builder::Separated,
    sqlite::Sqlite
};

use crate::db::{DatabaseError, ProjectSummaryRow};

// TODO: put a QueryBuilder into the db object for reuse?

fn fts5_quote(s: &str) -> String {
    // Wrapping a query in double quotes ensures that it is interpreted as
    // a string in the FTS5 query langauge, but then internal double quotes
    // must also be escaped with an extra double quote.
    format!("\"{}\"", s.replace("\"", "\"\""))
}

fn join_proj(table: &str, n: usize) -> String {
    format!(" JOIN {table} AS {table}_{n} ON projects.project_id = {table}_{n}.project_id ")
}

fn join_users(table: &str, n: usize) -> String {
    format!(" JOIN users AS users_{n} ON users_{n}.user_id = {table}_{n}.user_id ")
}

const JOIN_FTS: &str = " JOIN projects_fts ON projects.project_id = projects_fts.rowid ";

trait JoinsExt {
    fn joins(&self) -> impl Iterator<Item = String>;
}

impl JoinsExt for &[Facet] {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.iter()
            .enumerate()
            .flat_map(|(i, f)| match f {
                Facet::Query(_) => vec![ JOIN_FTS.into() ],
                Facet::Tag(_) => vec![ join_proj("tags", i) ],
                Facet::Owner(_) => vec![
                    join_proj("owners", i),
                    join_users("owners", i)
                ],
                Facet::Player(_) => vec![
                    join_proj("players", i),
                    join_users("players", i)
                ],
                _ => vec![]
            })
    }
}

trait WhereExt<'args> {
    fn push_where(&mut self, i: usize, f: &'args Facet) -> &mut Self;
}

impl<'args, Sep> WhereExt<'args> for Separated<'_, 'args, Sqlite, Sep>
where
    Sep: std::fmt::Display
{
    fn push_where(&mut self, i: usize, f: &'args Facet) -> &mut Self {
        match f {
            Facet::Query(q) =>
                self.push(" projects_fts MATCH ")
                    .push_bind_unseparated(fts5_quote(q)),
            Facet::Publisher(p) =>
                self.push(" projects.game_publisher == ")
                    .push_bind_unseparated(p),
            Facet::Year(y) =>
                self.push(" projects.game_year == ")
                    .push_bind_unseparated(y),
            Facet::PlayersMin(m) =>
                self.push(" projects.game_players_min == ")
                    .push_bind_unseparated(m),
            Facet::PlayersMax(m) =>
                self.push(" projects.game_players_max == ")
                    .push_bind_unseparated(m),
            Facet::PlayersInc(m) =>
                self.push_bind(m)
                    .push_unseparated(" BETWEEN projects.game_players_min AND projects.game_players_max "),
            Facet::LengthMin(m) =>
                self.push(" projects.game_length_min >= ")
                    .push_bind_unseparated(m),
            Facet::LengthMax(m) =>
                self.push(" projects.game_length_max <= ")
                    .push_bind_unseparated(m),
            Facet::Tag(t) =>
                self.push(format!(" tags_{i}.tag == "))
                    .push_bind_unseparated(t),
            Facet::Owner(u) |
            Facet::Player(u) =>
                self.push(format!(" users_{i}.username == "))
                    .push_bind_unseparated(u)
        }
    }
}

pub async fn get_projects_count<'e, E>(
    ex: E,
    facets: &[Facet]
) -> Result<i64, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        match facets.len() {
            0 => {
                sqlx::query_scalar!(
                    "
SELECT COUNT(1)
FROM projects
                    "
                )
                .fetch_one(ex)
                .await?
            },
            1 if matches!(facets[0], Facet::Query(_)) => {
                // pure queries avoid joining on the projects table
                let Facet::Query(ref q) = facets[0] else { unreachable!() };

                let query = fts5_quote(q);

                sqlx::query_scalar!(
                    "
SELECT COUNT(1)
FROM projects_fts
WHERE projects_fts MATCH ?
                    ",
                    query
                )
                .fetch_one(ex)
                .await?
            },
            _ => {
                let mut qb = QueryBuilder::new(
                    "
SELECT COUNT(1)
FROM projects
                    "
                );

                for j in facets.joins() {
                    qb.push(j);
                }

                qb.push(" WHERE ");

                let mut qbs = qb.separated(" AND ");
                for (i, f) in facets.iter().enumerate() {
                    qbs.push_where(i, f);
                }

                qb
                    .build_query_scalar()
                    .fetch_one(ex)
                    .await?
            }
        }
    )
}

trait SortByField {
    fn field(&self) -> &'static str;
}

impl SortByField for SortBy {
    fn field(&self) -> &'static str {
        match self {
            SortBy::ProjectName => "projects.name COLLATE NOCASE",
            SortBy::GameTitle => "projects.game_title_sort COLLATE NOCASE",
            SortBy::ModificationTime => "projects.modified_at",
            SortBy::CreationTime => "projects.created_at",
            SortBy::Relevance => "projects_fts.rank"
        }
    }
}

trait DirectionStrs {
    fn dir(&self) -> &'static str;

    fn op(&self) -> &'static str;
}

impl DirectionStrs for Direction {
    fn dir(&self) -> &'static str {
        match self {
            Direction::Ascending => "ASC",
            Direction::Descending => "DESC"
        }
    }

    fn op(&self) -> &'static str {
        match self {
            Direction::Ascending => ">",
            Direction::Descending => "<"
        }
    }
}

const SUMMARY_FIELDS: &str = "
    projects.project_id,
    projects.name,
    projects.slug,
    projects.description,
    projects.revision,
    projects.created_at,
    projects.modified_at,
    projects.game_title,
    projects.game_title_sort,
    projects.game_publisher,
    projects.game_year,
    projects.game_players_min,
    projects.game_players_max,
    projects.game_length_min,
    projects.game_length_max,
    projects.image
";

const WINDOW_SELECT: &str = formatcp!("
SELECT
    0.0 AS rank,
    {SUMMARY_FIELDS}
FROM projects
");

const WINDOW_SELECT_FTS: &str = formatcp!("
SELECT
    projects_fts.rank,
    {SUMMARY_FIELDS}
FROM projects
");

pub async fn get_projects_end_window<'e, E>(
    ex: E,
    facets: &[Facet],
    sort_by: SortBy,
    dir: Direction,
    limit: u32
) -> Result<Vec<ProjectSummaryRow>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>
{
    Ok(
        match facets.len() {
            0 => QueryBuilder::new(formatcp!("{WINDOW_SELECT} ORDER BY "))
                .push(sort_by.field())
                .push(" ")
                .push(dir.dir())
                .push(", project_id ")
                .push(dir.dir())
                .push(" LIMIT ")
                .push_bind(limit)
                .build_query_as::<ProjectSummaryRow>()
                .fetch_all(ex)
                .await?,
            _ => {
                let mut qb = QueryBuilder::new(
                    if facets.iter().any(|f| matches!(f, Facet::Query(_))) {
                        WINDOW_SELECT_FTS
                    }
                    else {
                        WINDOW_SELECT
                    }
                );

                for j in facets.joins() {
                    qb.push(j);
                }

                qb.push(" WHERE ");

                let mut qbs = qb.separated(" AND ");
                for (i, f) in facets.iter().enumerate() {
                    qbs.push_where(i, f);
                }

                qb
                    .push(" ORDER BY ")
                    .push(sort_by.field())
                    .push(" ")
                    .push(dir.dir())
                    .push(", projects.project_id ")
                    .push(dir.dir())
                    .push(" LIMIT ")
                    .push_bind(limit)
                    .build_query_as::<ProjectSummaryRow>()
                    .fetch_all(ex)
                    .await?
            }
        }
    )
}

pub async fn get_projects_mid_window<'e, 'f, E, F>(
    ex: E,
    facets: &'f [Facet],
    sort_by: SortBy,
    dir: Direction,
    field: &'f F,
    id: u32,
    limit: u32
) -> Result<Vec<ProjectSummaryRow>, DatabaseError>
where
    E: Executor<'e, Database = Sqlite>,
    F: Send + Sync + Encode<'f, Sqlite> + Type<Sqlite>
{
    Ok(
        match facets.len() {
            0 => QueryBuilder::new(formatcp!("{WINDOW_SELECT} WHERE "))
                .push(sort_by.field())
                .push(" ")
                .push(dir.op())
                .push(" ")
                .push_bind(field)
                .push(" OR (")
                .push(sort_by.field())
                .push(" = ")
                .push_bind(field)
                .push(" AND project_id ")
                .push(dir.op())
                .push(" ")
                .push_bind(id)
                .push(") ORDER BY ")
                .push(sort_by.field())
                .push(" ")
                .push(dir.dir())
                .push(", project_id ")
                .push(dir.dir())
                .push(" LIMIT ")
                .push_bind(limit)
                .build_query_as::<ProjectSummaryRow>()
                .fetch_all(ex)
                .await?,
            _ => {
                let mut qb = QueryBuilder::new(
                    if facets.iter().any(|f| matches!(f, Facet::Query(_))) {
                        WINDOW_SELECT_FTS
                    }
                    else {
                        WINDOW_SELECT
                    }
                );

                for j in facets.joins() {
                    qb.push(j);
                }

                qb.push(" WHERE ");

                let mut qbs = qb.separated(" AND ");
                for (i, f) in facets.iter().enumerate() {
                    qbs.push_where(i, f);
                }

                qb
                    .push(" AND (")
                    .push(sort_by.field())
                    .push(" ")
                    .push(dir.op())
                    .push(" ")
                    .push_bind(field)
                    .push(" OR (")
                    .push(sort_by.field())
                    .push(" = ")
                    .push_bind(field)
                    .push(" AND projects.project_id ")
                    .push(dir.op())
                    .push(" ")
                    .push_bind(id)
                    .push(")) ORDER BY ")
                    .push(sort_by.field())
                    .push(" ")
                    .push(dir.dir())
                    .push(", projects.project_id ")
                    .push(dir.dir())
                    .push(" LIMIT ")
                    .push_bind(limit)
                    .build_query_as::<ProjectSummaryRow>()
                    .fetch_all(ex)
                    .await?
            }
        }
    )
}

#[cfg(test)]
mod test {
    use super::*;

    type Pool = sqlx::Pool<Sqlite>;

    #[test]
    fn fts5_quote_abc() {
        assert_eq!("\"abc\"", fts5_quote("abc"));
    }

    #[test]
    fn fts5_quote_abc_def() {
        assert_eq!("\"abc def\"", fts5_quote("abc def"));
    }

    #[test]
    fn fts5_quote_abcq_def() {
        assert_eq!("\"abc\"\" def\"", fts5_quote("abc\" def"));
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_count_ok(pool: Pool) {
        assert_eq!(get_projects_count(&pool, &[]).await.unwrap(), 2);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_query_count_one(pool: Pool) {
        let facets = [
            Facet::Query("Another".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_query_count_zero(pool: Pool) {
        let facets = [
            Facet::Query("xxx".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 0);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_facet_count_one(pool: Pool) {
        let facets = [
            Facet::Publisher("XYZ".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_facet_count_zero(pool: Pool) {
        let facets = [
            Facet::Publisher("zzz".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 0);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_facet_count_multi_one(pool: Pool) {
        let facets = [
            Facet::Publisher("XYZ".into()),
            Facet::Year("1993".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn get_projects_facet_count_join_one(pool: Pool) {
        let facets = [
            Facet::Publisher("Test Game Company".into()),
            Facet::Owner("bob".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_facet_query_count_one(pool: Pool) {
        let facets = [
            Facet::Query("Another".into()),
            Facet::Publisher("XYZ".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_facet_query_count_query_zero(pool: Pool) {
        let facets = [
            Facet::Query("xxx".into()),
            Facet::Publisher("XYZ".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 0);
    }

    #[sqlx::test(fixtures("users", "projects"))]
    async fn get_projects_facet_query_count_facet_zero(pool: Pool) {
        let facets = [
            Facet::Query("Another".into()),
            Facet::Publisher("zzz".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 0);
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_projects_facet_count_tag(pool: Pool) {
        let facets = [
            Facet::Tag("a".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 2);
    }

    #[sqlx::test(fixtures("users", "projects", "tags"))]
    async fn get_projects_facet_count_two_tags(pool: Pool) {
        let facets = [
            Facet::Tag("a".into()),
            Facet::Tag("b".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners"))]
    async fn get_projects_facet_count_owner(pool: Pool) {
        let facets = [
            Facet::Owner("bob".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 2);
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners"))]
    async fn get_projects_facet_count_two_owners(pool: Pool) {
        let facets = [
            Facet::Owner("alice".into()),
            Facet::Owner("bob".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 2);
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn get_projects_facet_count_player(pool: Pool) {
        let facets = [
            Facet::Player("bob".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners", "players"))]
    async fn get_projects_facet_count_two_players(pool: Pool) {
        let facets = [
            Facet::Player("alice".into()),
            Facet::Player("bob".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners", "players"))]
    async fn get_projects_facet_count_owner_player(pool: Pool) {
        let facets = [
            Facet::Owner("alice".into()),
            Facet::Player("bob".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[sqlx::test(fixtures("users", "projects", "tags", "two_owners", "players"))]
    async fn get_projects_facet_count_many(pool: Pool) {
        let facets = [
            Facet::Query("Trademarked".into()),
            Facet::Publisher("Test Game Company".into()),
            Facet::Tag("a".into()),
            Facet::Owner("bob".into()),
            Facet::Player("bob".into())
        ];
        assert_eq!(get_projects_count(&pool, &facets).await.unwrap(), 1);
    }

    #[track_caller]
    fn assert_projects_window(
        act: Result<Vec<ProjectSummaryRow>, DatabaseError>,
        exp: &[&str]
    )
    {
        assert_eq!(
            act.unwrap().into_iter().map(|r| r.name).collect::<Vec<_>>(),
            exp
        );
    }

    #[sqlx::test]
    async fn get_projects_end_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Ascending,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_end_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Ascending,
                3
            ).await,
            &["a", "b", "c"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_end_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Ascending,
                5
            ).await,
            &["a", "b", "c", "d"]
        );
    }

    #[sqlx::test]
    async fn get_projects_end_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Descending,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_end_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Descending,
                3
            ).await,
            &["d", "c", "b"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_end_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Descending,
                5
            ).await,
            &["d", "c", "b", "a"]
        );
    }

    #[sqlx::test]
    async fn get_projects_mid_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Ascending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_mid_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Ascending,
                &"b",
                2,
                3
            ).await,
            &["c", "d"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_mid_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Ascending,
                &"d",
                4,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test]
    async fn get_projects_mid_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Descending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_mid_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Descending,
                &"b",
                2,
                3
            ).await,
            &["a"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_window"))]
    async fn get_projects_mid_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[],
                SortBy::ProjectName,
                Direction::Descending,
                &"d",
                4,
                3
            ).await,
            &["c", "b", "a"]
        );
    }

    #[sqlx::test]
    async fn get_projects_facet_end_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                3
            ).await,
            &[]
        );
    }

   #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_end_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                1
            ).await,
            &["a"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_end_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                5
            ).await,
            &["a", "c"]
        );
    }

    #[sqlx::test]
    async fn get_projects_facet_end_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_end_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                1
            ).await,
            &["c"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_end_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                5
            ).await,
            &["c", "a"]
        );
    }

    #[sqlx::test]
    async fn get_projects_facet_mid_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_mid_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                &"b",
                2,
                3
            ).await,
            &["c"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_mid_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                &"d",
                4,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test]
    async fn get_projects_facet_mid_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_mid_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                &"d",
                4,
                1
            ).await,
            &["c"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_mid_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Publisher("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                &"d",
                4,
                5
            ).await,
            &["c", "a"]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_end_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_end_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                1
            ).await,
            &["a"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_end_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                5
            ).await,
            &["a", "c", "d"]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_end_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_end_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                1
            ).await,
            &["d"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_end_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                5
            ).await,
            &["d", "c", "a"]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_mid_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_mid_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                &"b",
                2,
                3
            ).await,
            &["c", "d"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_mid_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Ascending,
                &"d",
                4,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_mid_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_mid_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                &"d",
                4,
                1
            ).await,
            &["c"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_mid_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[Facet::Query("abc".into())],
                SortBy::ProjectName,
                Direction::Descending,
                &"d",
                4,
                5
            ).await,
            &["c", "a"]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_facet_end_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Year("1979".into())
                ],
                SortBy::ProjectName,
                Direction::Ascending,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_end_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Ascending,
                1
            ).await,
            &["a"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_end_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Ascending,
                5
            ).await,
            &["a", "c"]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_facet_end_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Descending,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_end_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Descending,
                1
            ).await,
            &["c"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_end_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Descending,
                5
            ).await,
            &["c", "a"]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_facet_mid_window_asc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Ascending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_mid_window_asc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Ascending,
                &"b",
                2,
                3
            ).await,
            &["c"]
        );
    }
    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_mid_window_asc_past_end(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Ascending,
                &"d",
                4,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test]
    async fn get_projects_query_facet_mid_window_desc_empty(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Descending,
                &"a",
                1,
                3
            ).await,
            &[]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_mid_window_desc_not_all(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Descending,
                &"d",
                4,
                1
            ).await,
            &["c"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_query_facet_mid_window_desc_past_start(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into())
                ],
                SortBy::ProjectName,
                Direction::Descending,
                &"d",
                4,
                5
            ).await,
            &["c", "a"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_end_window_many(pool: Pool) {
        assert_projects_window(
            get_projects_end_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into()),
                    Facet::Tag("a".into()),
                    Facet::Owner("bob".into()),
                    Facet::Player("bob".into())
                ],
                SortBy::ProjectName,
                Direction::Ascending,
                5
            ).await,
            &["a"]
        );
    }

    #[sqlx::test(fixtures("users", "proj_facet_window"))]
    async fn get_projects_facet_mid_window_many(pool: Pool) {
        assert_projects_window(
            get_projects_mid_window(
                &pool,
                &[
                    Facet::Query("abc".into()),
                    Facet::Publisher("abc".into()),
                    Facet::Tag("a".into()),
                    Facet::Owner("bob".into()),
                    Facet::Player("bob".into())
                ],
                SortBy::ProjectName,
                Direction::Descending,
                &"b",
                2,
                5
            ).await,
            &["a"]
        );
    }
}
