#![feature(async_fn_track_caller)]

use axum::{
    Router, serve,
    body::Body,
    extract::{ConnectInfo, Request},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, patch, post, put}
};
use chrono::Utc;
use futures_util::future::try_join_all;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqlitePoolOptions;
use std::{
    fs,
    io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration
};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    timeout::TimeoutLayer,
    trace::{DefaultOnFailure, DefaultOnResponse, MakeSpan, TraceLayer}
};
use tracing::{error, info, info_span, warn, Level, Span};
use tracing_panic::panic_hook;
use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::SubscriberInitExt
};

mod app;
mod config;
mod content_type;
mod core;
mod db;
mod errors;
mod extractors;
mod handlers;
mod input;
mod jwt;
mod model;
mod module;
mod params;
mod prod_core;
mod sqlite;
mod time;
mod upload;
mod version;

use crate::{
    app::AppState,
    config::Config,
    core::{Core, CoreArc},
    prod_core::ProdCore,
    errors::AppError,
    jwt::DecodingKey,
    sqlite::SqlxDatabaseClient,
    upload::{BucketUploader, LocalUploader},
};

impl From<&AppError> for StatusCode {
    fn from(err: &AppError) -> Self {
        match err {
            AppError::BadMimeType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            AppError::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::CannotRemoveLastOwner => StatusCode::BAD_REQUEST,
            AppError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::UploadError(_) => StatusCode::BAD_REQUEST,
            AppError::ModuleError(_) => StatusCode::BAD_REQUEST,
            AppError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::JsonError => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::LimitOutOfRange => StatusCode::BAD_REQUEST,
            AppError::InvalidProjectName => StatusCode::BAD_REQUEST,
            AppError::InvalidPackageName => StatusCode::BAD_REQUEST,
            AppError::AlreadyExists => StatusCode::BAD_REQUEST,
            AppError::MalformedQuery => StatusCode::BAD_REQUEST,
            AppError::MalformedUpload => StatusCode::BAD_REQUEST,
            AppError::MalformedVersion => StatusCode::BAD_REQUEST,
            AppError::NotAUser => StatusCode::NOT_FOUND,
            AppError::NotFound => StatusCode::NOT_FOUND,
            AppError::NotEmpty => StatusCode::BAD_REQUEST,
            AppError::Forbidden => StatusCode::FORBIDDEN,
            AppError::Unauthorized => StatusCode::UNAUTHORIZED
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct HttpError {
    error: String
}

impl From<AppError> for HttpError {
    fn from(err: AppError) -> Self {
        HttpError { error: format!("{}", err) }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let code = StatusCode::from(&self);

        // Log errors
        if code.is_server_error() {
            error!("{}", self);
        }
        else {
            warn!("{}", self);
        }

        let body = Json(HttpError::from(self));
        (code, body).into_response()
    }
}

fn real_addr(request: &Request) -> String {
    // If we're behind a proxy, get IP from X-Forwarded-For header
    match request.headers().get("x-forwarded-for") {
        Some(addr) => addr.to_str()
            .map(String::from)
            .ok(),
        None => request.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|info| info.ip().to_string())
    }
    .unwrap_or_else(|| "<unknown>".into())
}

#[derive(Clone, Debug)]
struct SpanMaker {
    include_headers: bool
}

impl SpanMaker {
    pub fn new() -> Self {
        Self { include_headers: false }
    }

    pub fn include_headers(mut self, include_headers: bool) -> Self {
        self.include_headers = include_headers;
        self
    }
}

impl MakeSpan<Body> for SpanMaker {
    fn make_span(&mut self, request: &Request<Body>) -> Span {
        if self.include_headers {
            info_span!(
                "request",
                source = %real_addr(request),
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version(),
                headers = ?request.headers()
            )
        }
        else {
            info_span!(
                "request",
                source = %real_addr(request),
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version()
            )
        }
    }
}

fn routes(
    api: &str,
    read_only: bool,
    log_headers: bool,
    upload_timeout: u64
) -> Router<AppState> {
    // set up our routes under api
    let api_router = Router::new()
        .route(
            "/projects",
            get(handlers::projects_get)
        )
        .route(
            "/projects/{proj}",
            if read_only {
                post(handlers::forbidden)
                .patch(handlers::forbidden)
            }
            else {
                post(handlers::project_post)
                .patch(handlers::project_patch)
            }
            .get(handlers::project_get)
        )
        .route(
            "/projects/{proj}/{revision}",
            get(handlers::project_revision_get)
        )
        .route(
            "/projects/{proj}/owners",
            if read_only {
                put(handlers::forbidden)
                .delete(handlers::forbidden)
            }
            else {
                put(handlers::owners_add)
                .delete(handlers::owners_remove)
            }
            .get(handlers::owners_get)
        )
        .route(
            "/projects/{proj}/players",
            if read_only {
                put(handlers::forbidden)
                .delete(handlers::forbidden)
            }
            else {
                put(handlers::players_add)
                .delete(handlers::players_remove)
            }
            .get(handlers::players_get)
        )
        .route(
            "/projects/{proj}/packages/{pkg_name}",
            if read_only {
                post(handlers::forbidden)
                .patch(handlers::forbidden)
                .delete(handlers::forbidden)
            }
            else {
                post(handlers::package_post)
                .patch(handlers::package_patch)
                .delete(handlers::package_delete)
            }
        )
        .route(
            "/projects/{proj}/packages/{pkg_name}/{version}",
            if read_only {
                post(handlers::forbidden)
                .delete(handlers::forbidden)
            }
            else {
// FIXME: release_version_post?
                post(handlers::release_post)
                .delete(handlers::release_delete)
            }
        )
        .route(
            "/projects/{proj}/gallery",
            if read_only {
                patch(handlers::forbidden)
            }
            else {
                patch(handlers::gallery_patch)
            }
        )
        .route(
            "/projects/{proj}/gallery/{img_name}",
            if read_only {
                post(handlers::forbidden)
            }
            else {
                post(handlers::gallery_post)
            }
        )
        .route(
            "/projects/{proj}/images/{img_name}",
            if read_only {
                post(handlers::forbidden)
            }
            else {
                post(handlers::image_post)
            }
            .get(handlers::image_get)
        )
        .route(
            "/projects/{proj}/images/{img_name}/{revision}",
            get(handlers::image_revision_get)
        )
        .route(
            "/projects/{proj}/flag",
            if read_only {
                post(handlers::forbidden)
            }
            else {
                post(handlers::flag_post)
            }
        )
        .route(
            "/publishers",
            get(handlers::publishers_get)
        )
        .route(
            "/tags",
             get(handlers::tags_get)
        )
        .route(
            "/admin/flags",
            get(handlers::admin_flags_get)
        )
        .route(
            "/admin/flags/{flag}",
            if read_only {
                patch(handlers::forbidden)
            }
            else {
                patch(handlers::admin_flag_close)
            }
        )
        .layer(TimeoutLayer::new(Duration::from_secs(10)))
        .route(
            "/projects/{proj}/packages/{pkg_name}/{version}/{file}",
            if read_only {
                post(handlers::forbidden)
            }
            else {
                post(handlers::file_post)
                    .layer(TimeoutLayer::new(
                        Duration::from_secs(upload_timeout))
                    )
            }
        );

    // set up things wrapped around our routes
    Router::new()
        .route(
            &format!("{api}/"),
            get(handlers::root_get)
        )
        .nest(api, api_router)
        .fallback(handlers::not_found)
        .layer(
            ServiceBuilder::new()
                .layer(CorsLayer::very_permissive())
                .layer(CompressionLayer::new())
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(SpanMaker::new().include_headers(log_headers))
                .on_response(DefaultOnResponse::new().level(Level::INFO))
                .on_failure(DefaultOnFailure::new().level(Level::WARN))
        )
}

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("{0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("{0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("{0}")]
    Database(#[from] sqlx::Error),
    #[error("{0}")]
    AdminUsernameError(#[from] core::GetIdError),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    BucketUploader(#[from] upload::BucketUploaderError),
    #[error("Uploads directory does not exist")]
    NoUploadsDirectory
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut interrupt = signal(SignalKind::interrupt())
        .expect("failed to install signal handler");

    // Docker sends SIGQUIT for some unfathomable reason
    let mut quit = signal(SignalKind::quit())
        .expect("failed to install signal handler");

    let mut terminate = signal(SignalKind::terminate())
        .expect("failed to install signal handler");

    tokio::select! {
        _ = interrupt.recv() => info!("received SIGINT"),
        _ = quit.recv() => info!("received SIGQUIT"),
        _ = terminate.recv() => info!("received SIGTERM")
    }
}

async fn run() -> Result<(), StartupError> {
    info!("Reading config.toml");
    let config: Config = toml::from_str(&fs::read_to_string("config.toml")?)?;

    let upload_dir = PathBuf::from(config.upload_dir);
    if !upload_dir.is_dir() {
        return Err(StartupError::NoUploadsDirectory);
    }

    info!("Opening database {}", config.db_path);
    let db_pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&format!("sqlite://{}", &config.db_path))
        .await?;

    let core = ProdCore {
        db: SqlxDatabaseClient(db_pool),
//        uploader: LocalUploader { uploads_directory: "uploads".into() },
        uploader: BucketUploader::new(
            &config.bucket_name,
            &config.bucket_region,
            &config.bucket_endpoint,
            &config.bucket_access_key,
            &config.bucket_secret_key,
            &config.bucket_base_url,
            &config.bucket_base_dir
        )?,
        now: Utc::now,
        max_image_size: config.max_image_size << 20, // MB to bytes
        max_file_size: config.max_file_size << 20,   // MB to bytes
        upload_dir
    };

    let mut admins = try_join_all(
        config.admins.iter().map(|username| core.get_user_id(username))
    ).await?;

    admins.sort();

    let state = AppState {
        key: DecodingKey::from_secret(config.jwt_key.as_bytes()),
        core: Arc::new(core) as CoreArc,
        admins
    };

    let app: Router = routes(
        &config.api_base_path,
        config.read_only,
        config.log_headers,
        config.upload_timeout
    ).with_state(state);

    let ip: IpAddr = config.listen_ip.parse()?;
    let addr = SocketAddr::from((ip, config.listen_port));
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>()
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    // set up logging
    // TODO: make log location configurable
    let file_appender = tracing_appender::rolling::daily("", "gls.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| {
                [
                    // log this crate at info level
                    &format!("{}=info", env!("CARGO_CRATE_NAME")),
                    // tower_http is noisy below info
                    "tower_http=info",
                    // axum::rejection=trace shows rejections from extractors
                    "axum::rejection=trace",
                    // every panic is a fatal error
                    "tracing_panic=error"
                ].join(",").into()
            })
        )
        .with(tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_writer(non_blocking)
        )
        .init();

    // ensure that panics are logged
    std::panic::set_hook(Box::new(panic_hook));

    info!("Starting");

    if let Err(e) = run().await {
        error!("{}", e);
    }

    info!("Exiting");
}

#[cfg(test)]
mod test {
    use super::*;

    use async_trait::async_trait;
    use axum::{
        body::{self, Body, Bytes},
        http::{
            Method, Request,
            header::{ACCEPT_ENCODING, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, LOCATION}
        }
    };
    use futures::Stream;
    use glc::{
        model::{FlagData, Flags, FlagTag, GameData, FileData, PackageData, ProjectData, Projects, ProjectSummary, Range, ReleaseData, Users},
        pagination::{Anchor, Direction, Limit, SortBy, Pagination, Seek, SeekLink}
    };
    use mime::{APPLICATION_JSON, IMAGE_PNG, TEXT_PLAIN, Mime};
    use once_cell::sync::Lazy;
    use nix::{
        sys::{self, signal::Signal},
        unistd::Pid
    };
    use std::future::IntoFuture;
    use tokio_util::io::StreamReader;
    use tower::ServiceExt; // for oneshot

    use crate::{
        core::{AddFileError, AddFlagError, AddImageError, AddOwnersError, AddPlayerError, CloseFlagError, Core, CreatePackageError, CreateProjectError, CreateReleaseError, DeletePackageError, DeleteReleaseError, GetFlagsError, GetIdError, GetImageError, GetOwnersError, GetPlayersError, GetProjectError, GetProjectsError, RemoveOwnersError, RemovePlayerError, UpdateGalleryError, UpdatePackageError, UpdateProjectError, UserIsOwnerError},
        input::{FlagPost, GalleryPatch, GalleryOp, GameDataPost, PackageDataPatch, PackageDataPost, ProjectDataPatch, ProjectDataPost, RangePost},
        jwt::{self, EncodingKey},
        model::{Admin, Flag, Owner, Package, Project, Release, User},
        params::ProjectsParams
    };

    const API_V1: &str = "/api/v1";
    const KEY: &[u8] = b"@wlD+3L)EHdv28u)OFWx@83_*TxhVf9IdUncaAz6ICbM~)j+dH=sR2^LXp(tW31z";

    async fn body_bytes(r: Response) -> Bytes {
        body::to_bytes(r.into_body(), usize::MAX).await.unwrap()
    }

    async fn body_as<D: for<'a> Deserialize<'a>>(r: Response) -> D {
        serde_json::from_slice::<D>(&body_bytes(r).await).unwrap()
    }

    async fn body_empty(r: Response) -> bool {
        body_bytes(r).await.is_empty()
    }

    static PROJECT_SUMMARY_A: Lazy<ProjectSummary> = Lazy::new(||
        ProjectSummary {
            name: "project_a".into(),
            slug: "project_a".into(),
            description: "whatever".into(),
            revision: 1,
            created_at: "2024-03-29T16:51:08Z".into(),
            modified_at: "2024-03-29T16:51:08Z".into(),
            tags: vec![],
            game: GameData {
                title: "a".into(),
                title_sort_key: "a".into(),
                publisher: "p".into(),
                year: "2024".into(),
                players: Range::default(),
                length: Range::default()
            }
        }
    );

    static PROJECT_SUMMARY_B: Lazy<ProjectSummary> = Lazy::new(||
        ProjectSummary {
            name: "project_b".into(),
            slug: "project_b".into(),
            description: "la la la".into(),
            revision: 1,
            created_at: "2024-03-29T17:00:23Z".into(),
            modified_at: "2024-03-29T17:00:23Z".into(),
            tags: vec![],
            game: GameData {
                title: "b".into(),
                title_sort_key: "b".into(),
                publisher: "p".into(),
                year: "2024".into(),
                players: Range::default(),
                length: Range::default()
            }
        }
    );

    const BOB_UID: i64 = 1;

    static EIA_PROJECT_DATA: Lazy<ProjectData> = Lazy::new(||
        ProjectData {
            name: "eia".into(),
            slug: "eia".into(),
            description: "A module for Empires in Arms".into(),
            revision: 1,
            created_at: "2023-10-26T00:00:00.000000000Z".into(),
            modified_at: "2023-10-30T18:53:53.056386142Z".into(),
            tags: vec![],
            game: GameData {
                title: "Empires in Arms".into(),
                title_sort_key: "Empires in Arms".into(),
                publisher: "Avalon Hill".into(),
                year: "1983".into(),
                players: Range::default(),
                length: Range::default()
            },
            readme: "".into(),
            image: None,
            owners: vec!["alice".into(), "bob".into()],
            packages: vec![
                PackageData {
                    name: "a_package".into(),
                    slug: "a_package".into(),
                    sort_key: 0,
                    description: "Some package".into(),
                    releases: vec![
                        ReleaseData {
                            version: "1.2.3".into(),
                            files: vec![
                                FileData {
                                    filename: "eia.vmod".into(),
                                    url: "https://example.com/eia.vmod".into(),
                                    size: 0,
                                    sha256: "deadbeef".into(),
                                    published_at: "2023-10-30T18:53:53.056386142Z".into(),
                                    published_by: "alice".into(),
                                    requires: None
                                }
                            ]
                        }
                    ],
                }
            ],
            gallery: vec![]
        }
    );

    const MAX_FILE_SIZE: usize = 256;

    const MAX_IMAGE_SIZE: usize = 256;

    async fn exhaust_stream(
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
    ) -> Result<(), io::Error>
    {
        let mut reader = StreamReader::new(stream);
        let mut writer = tokio::io::empty();

        tokio::io::copy(&mut reader, &mut writer)
            .await
            .map(|_| ())
    }

    #[derive(Clone)]
    struct TestCore { }

    #[async_trait]
    impl Core for TestCore {
        fn max_file_size(&self) -> usize { MAX_FILE_SIZE }

        fn max_image_size(&self) -> usize { MAX_IMAGE_SIZE }

        async fn get_project_id(
            &self,
            proj: &str,
        ) -> Result<Project, GetIdError>
        {
            match proj {
                "a_project" => Ok(Project(1)),
                _ => Err(GetIdError::NotFound)
            }
        }

        async fn get_package_id(
            &self,
            _proj: Project,
            pkg: &str
        ) -> Result<Package, GetIdError>
        {
            match pkg {
                "a_package" => Ok(Package(1)),
                "nonempty" => Ok(Package(2)),
                _ => Err(GetIdError::NotFound)
            }
        }

        async fn get_project_package_ids(
            &self,
            proj: &str,
            pkg: &str
        ) -> Result<(Project, Package), GetIdError>
        {
            match (proj, pkg) {
                ("a_project", "a_package") => Ok((Project(1), Package(1))),
                ("a_project", "nonempty") => Ok((Project(1), Package(2))),
                _ => Err(GetIdError::NotFound)
            }
        }

        async fn get_release_id(
             &self,
            _proj: Project,
            _pkg: Package,
            release: &str
        ) -> Result<Release, GetIdError>
        {
            match release {
                "1.2.3" => Ok(Release(1)),
                "1.2.4" => Ok(Release(2)),
                _ => Err(GetIdError::NotFound)
            }
        }

        async fn get_project_package_release_ids(
            &self,
            proj: &str,
            pkg: &str,
            release: &str
        ) -> Result<(Project, Package, Release), GetIdError>
        {
            match (proj, pkg, release) {
                ("a_project", "a_package", "1.2.3") =>
                    Ok((Project(1), Package(1), Release(1))),
                ("a_project", "a_package", "1.2.4") =>
                    Ok((Project(1), Package(1), Release(2))),
                _ => Err(GetIdError::NotFound)
            }
        }

        async fn user_is_owner(
            &self,
            user: User,
            _proj: Project
        ) -> Result<bool, UserIsOwnerError>
        {
            Ok(user == User(1) || user == User(2))
        }

        async fn add_owners(
            &self,
            _owners: &Users,
            _proj: Project
        ) -> Result<(), AddOwnersError>
        {
            Ok(())
        }

        async fn remove_owners(
            &self,
            _owners: &Users,
            _proj: Project
        ) -> Result<(), RemoveOwnersError>
        {
            Ok(())
        }

        async fn get_owners(
            &self,
            _proj: Project
        ) -> Result<Users, GetOwnersError>
        {
            Ok(
                Users {
                    users: vec![
                        "alice".into(),
                        "bob".into()
                    ]
                }
            )
        }

        async fn get_projects(
            &self,
            params: ProjectsParams
        ) -> Result<Projects, GetProjectsError>
        {
            Ok(
                Projects {
                    projects: vec![
                        PROJECT_SUMMARY_A.clone(),
                        PROJECT_SUMMARY_B.clone()
                    ],
                    meta: Pagination {
                        prev_page: Some(
                            SeekLink::new(
                                &Seek {
                                    anchor: Anchor::Before("project_a".into(), 0),
                                    sort_by: SortBy::ProjectName,
                                    dir: Direction::Ascending,
                                    facets: vec![]
                                },
                                params.limit
                            )
                        ),
                        next_page: Some(
                            SeekLink::new(
                                &Seek {
                                    anchor: Anchor::After("project_b".into(), 0),
                                    sort_by: SortBy::ProjectName,
                                    dir: Direction::Ascending,
                                    facets: vec![]
                                },
                                params.limit
                            )
                        ),
                        total: 1234
                    }
                }
            )
        }

        async fn get_project(
            &self,
            _proj: Project,
        ) -> Result<ProjectData, GetProjectError>
        {
            Ok(EIA_PROJECT_DATA.clone())
        }

        async fn create_project(
            &self,
            _user: User,
            _proj: &str,
            _proj_data: &ProjectDataPost
        ) -> Result<(), CreateProjectError>
        {
            Ok(())
        }

        async fn update_project(
            &self,
            _owner: Owner,
            _proj: Project,
            _proj_data: &ProjectDataPatch
        ) -> Result<(), UpdateProjectError>
        {
            Ok(())
        }

        async fn get_project_revision(
            &self,
            proj: Project,
            revision: i64
        ) -> Result<ProjectData, GetProjectError>
        {
            match revision {
                1 => self.get_project(proj).await,
                _ => Err(GetProjectError::NotFound)
            }
        }

        async fn create_package(
            &self,
            _owner: Owner,
            _proj: Project,
            pkg: &str,
            _pkg_data: &PackageDataPost
        ) -> Result<(), CreatePackageError>
        {
            match pkg {
                "newpkg" => Ok(()),
                _ => Err(CreatePackageError::AlreadyExists)
            }
        }

        async fn update_package(
            &self,
            _owner: Owner,
            _proj: Project,
            _pkg: Package,
            _pkg_data: &PackageDataPatch
        ) -> Result<(), UpdatePackageError>
        {
            Ok(())
        }

        async fn delete_package(
            &self,
            _owner: Owner,
            _proj: Project,
            pkg: Package,
        ) -> Result<(), DeletePackageError>
        {
            match pkg {
                Package(1) => Ok(()),
                _ => Err(DeletePackageError::NotEmpty)
            }
        }

        async fn create_release(
            &self,
            _owner: Owner,
            proj: Project,
            pkg: Package,
            version: &str,
        ) -> Result<(), CreateReleaseError>
        {
            match (proj, pkg, version) {
                (Project(1), Package(1), "1.2.4") => Ok(()),
                _ => Err(CreateReleaseError::AlreadyExists)
            }
        }

        async fn delete_release(
            &self,
            _owner: Owner,
            _proj: Project,
            rel: Release
        ) -> Result<(), DeleteReleaseError>
        {
            match rel {
                Release(1) => Ok(()),
                _ => Err(DeleteReleaseError::NotEmpty)
            }
        }

        async fn get_players(
            &self,
            _proj: Project
        ) -> Result<Users, GetPlayersError>
        {
            Ok(
                Users {
                    users: vec![
                        "player 1".into(),
                        "player 2".into()
                    ]
                }
            )
        }

        async fn add_player(
            &self,
            _player: User,
            _proj: Project
        ) -> Result<(), AddPlayerError>
        {
            Ok(())
        }

        async fn remove_player(
            &self,
            _player: User,
            _proj: Project
        ) -> Result<(), RemovePlayerError>
        {
            Ok(())
        }

        async fn get_image(
            &self,
            proj: Project,
            img_name: &str
        ) -> Result<String, GetImageError>
        {
            if proj == Project(1) && img_name == "img.png" {
                Ok("https://example.com/img.png".into())
            }
            else {
                Err(GetImageError::NotFound)
            }
        }

        async fn add_image(
            &self,
            _owner: Owner,
            _proj: Project,
            _img_name: &str,
            content_type: &Mime,
            _content_length: Option<u64>,
            stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
        ) -> Result<(), AddImageError>
        {
            if content_type == &TEXT_PLAIN {
                Err(AddImageError::BadMimeType)
            }
            else {
                match exhaust_stream(stream).await {
                    Ok(_) => Ok(()),
                    Err(e) => match e.kind() {
                        io::ErrorKind::FileTooLarge => Err(AddImageError::TooLarge),
                        _ => Err(AddImageError::IOError(e))
                    }
                }
            }
        }

        async fn add_gallery_image(
            &self,
            _owner: Owner,
            _proj: Project,
            _img_name: &str,
            content_type: &Mime,
            _content_length: Option<u64>,
            stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
        ) -> Result<(), AddImageError>
        {
            if content_type == &TEXT_PLAIN {
                Err(AddImageError::BadMimeType)
            }
            else {
                match exhaust_stream(stream).await {
                    Ok(_) => Ok(()),
                    Err(e) => match e.kind() {
                        io::ErrorKind::FileTooLarge => Err(AddImageError::TooLarge),
                        _ => Err(AddImageError::IOError(e))
                    }
                }
            }
        }

         async fn update_gallery(
            &self,
            _owner: Owner,
            _proj: Project,
            _gallery_patch: &GalleryPatch
        ) -> Result<(), UpdateGalleryError>
        {
            Ok(())
        }

        async fn add_file(
            &self,
            _owner: Owner,
            _proj: Project,
            _release: Release,
            _filename: &str,
            _content_type: Option<&Mime>,
            _content_length: Option<u64>,
            stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
        ) -> Result<(), AddFileError>
        {
            match exhaust_stream(stream).await {
                Ok(_) => Ok(()),
                Err(e) => match e.kind() {
                    io::ErrorKind::FileTooLarge => Err(AddFileError::TooLarge),
                    _ => Err(AddFileError::IOError(e))
                }
            }
        }

        async fn get_flag_id(
            &self,
            flag: i64
        ) -> Result<Flag, GetIdError>
        {
            match flag {
                1 => Ok(Flag(1)),
                _ => Err(GetIdError::NotFound)
            }
        }

        async fn add_flag(
            &self,
            _reporter: User,
            _proj: Project,
            _flag: &FlagPost
        ) -> Result<(), AddFlagError>
        {
            Ok(())
        }

        async fn close_flag(
            &self,
            _admin: Admin,
            _flag: Flag
        ) -> Result<(), CloseFlagError>
        {
            Ok(())
        }

        async fn get_flags(
            &self
        ) -> Result<Flags, GetFlagsError>
        {
            Ok(
                Flags {
                    flags: vec![
                        FlagData {
                            flag_id: 1,
                            project: "a_project".into(),
                            slug: "a_project".into(),
                            flagged_by: "bob".into(),
                            flagged_at: "2023-10-30T18:53:53.056386142Z".into(),
                            flag: FlagTag::Spam,
                            message: None
                        }
                    ]
                }
            )
        }
    }

    fn test_state() -> AppState {
        AppState {
            key: DecodingKey::from_secret(KEY),
            core: Arc::new(TestCore {}) as CoreArc,
            admins: vec![User(5)]
        }
    }

    fn token(uid: i64) -> String {
        let ekey = EncodingKey::from_secret(KEY);
        let token = jwt::issue(&ekey, uid, 0, 899999999999).unwrap();
        format!("Bearer {token}")
    }

    async fn try_request(request: Request<Body>, rw: bool) -> Response {
        routes(API_V1, !rw, false, 10)
            .with_state(test_state())
            .oneshot(request)
            .await
            .unwrap()
    }

    fn headers<'a>(
        response: &'a Response,
        header_name: &str
    ) -> Vec<&'a [u8]>
    {
        let mut values = response
            .headers()
            .get_all(header_name)
            .iter()
            .flat_map(|v| v.as_ref().split(|b| b == &b','))
            .map(|v| if v[0] == b' ' { &v[1..] } else { v })
            .collect::<Vec<_>>();

        values.sort();
        values
    }

    #[track_caller]
    async fn assert_shutdown(sig: Signal) {
        let listener = TcpListener::bind("localhost:0").await.unwrap();
        let app = Router::new();
        let pid = Pid::this();

        let server_handle = tokio::spawn(
            serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .into_future()
        );

        // ensure that the server has a chance to start
        tokio::task::yield_now().await;

        sys::signal::kill(pid, sig).unwrap();

        server_handle.await.unwrap().unwrap();
    }

    #[track_caller]
    async fn assert_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_empty(response).await);
    }

    #[track_caller]
    async fn assert_forbidden(response: Response) {
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::Forbidden)
        );
    }

    #[track_caller]
    async fn assert_not_found(response: Response) {
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::NotFound)
        );
    }

    #[track_caller]
    async fn assert_malformed_query(response: Response) {
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::MalformedQuery)
        );
    }

    #[track_caller]
    async fn assert_limit_out_of_range(response: Response) {
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::LimitOutOfRange)
        );
    }

    #[track_caller]
    async fn assert_payload_too_large(response: Response) {
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::TooLarge)
        );
    }

    #[track_caller]
    async fn assert_unauthorized(response: Response) {
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::Unauthorized)
        );
    }

    #[track_caller]
    async fn assert_unsupported_media_type(response: Response) {
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::BadMimeType)
        );
    }

    #[track_caller]
    async fn assert_unprocessable_entity(response: Response) {
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            body_as::<HttpError>(response).await,
            HttpError::from(AppError::JsonError)
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_sigint() {
        assert_shutdown(Signal::SIGTERM).await;
    }

    #[tokio::test]
    async fn graceful_shutdown_sigquit() {
        assert_shutdown(Signal::SIGQUIT).await;
    }

    #[tokio::test]
    async fn graceful_shutdown_sigterm() {
        assert_shutdown(Signal::SIGTERM).await;
    }

    #[tokio::test]
    async fn cors_ok() {
        let response = try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/"))
                .body(Body::empty())
                .unwrap(),
            true
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);

        assert_eq!(
            headers(&response, "access-control-allow-credentials"),
            ["true".as_bytes()]
        );

        assert_eq!(
            headers(&response, "vary"),
            [
                "access-control-request-headers".as_bytes(),
                "access-control-request-method".as_bytes(),
                "origin".as_bytes()
            ]
        );
    }

    #[track_caller]
    async fn try_compression(comp: &str) {
        let response = try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects"))
                .header(ACCEPT_ENCODING, comp)
                .body(Body::empty())
                .unwrap(),
            true
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            headers(&response, "content-encoding"),
            [comp.as_bytes()]
        );
    }

    #[tokio::test]
    async fn compression_br() {
        try_compression("br").await;
    }

    #[tokio::test]
    async fn compression_deflate() {
        try_compression("deflate").await;
    }

    #[tokio::test]
    async fn compression_gzip() {
        try_compression("gzip").await;
    }

    #[tokio::test]
    async fn compression_zstd() {
        try_compression("zstd").await;
    }

    async fn get_bad_path(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/bogus/whatever"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_bad_path_rw() {
        let response = get_bad_path(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_bad_path_ro() {
        let response = get_bad_path(false).await;
        assert_not_found(response).await;
    }

    async fn get_root_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_root_ok_rw() {
        let response = get_root_ok(true).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(&body_bytes(response).await[..], b"hello world");
    }

    #[tokio::test]
    async fn get_root_ok_ro() {
        let response = get_root_ok(false).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(&body_bytes(response).await[..], b"hello world");
    }

    async fn get_projects_no_params_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_get_projects_no_params_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_no_params_ok_rw() {
        let response = get_projects_no_params_ok(true).await;
        assert_get_projects_no_params_ok(response).await;
    }

    #[tokio::test]
    async fn get_projects_no_params_ok_ro() {
        let response = get_projects_no_params_ok(false).await;
        assert_get_projects_no_params_ok(response).await;
    }

    async fn get_projects_limit_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects?limit=5"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_projects_limit_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            Limit::new(5)
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            Limit::new(5)
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_limit_ok_rw() {
        let response = get_projects_limit_ok(true).await;
        assert_projects_limit_ok(response).await;
    }

    #[tokio::test]
    async fn get_projects_limit_ok_ro() {
        let response = get_projects_limit_ok(false).await;
        assert_projects_limit_ok(response).await;
    }

    async fn get_projects_limit_zero(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects?limit=0"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_projects_limit_zero_rw() {
        let response = get_projects_limit_zero(true).await;
        assert_limit_out_of_range(response).await;
    }

    #[tokio::test]
    async fn get_projects_limit_zero_ro() {
        let response = get_projects_limit_zero(false).await;
        assert_limit_out_of_range(response).await;
    }

    async fn get_projects_limit_too_large(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects?limit=100000"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_projects_limit_too_large_rw() {
        let response = get_projects_limit_too_large(true).await;
        assert_limit_out_of_range(response).await;
    }

    #[tokio::test]
    async fn get_projects_limit_too_large_ro() {
        let response = get_projects_limit_too_large(false).await;
        assert_limit_out_of_range(response).await;
    }

    async fn get_projects_limit_empty(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects?limit="))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_projects_limit_empty_rw() {
        let response = get_projects_limit_empty(true).await;
        assert_get_projects_no_params_ok(response).await;
    }

    #[tokio::test]
    async fn get_projects_limit_empty_ro() {
        let response = get_projects_limit_empty(false).await;
        assert_get_projects_no_params_ok(response).await;
    }

    async fn get_projects_limit_not_a_number(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects?limit=eleventeen"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_projects_limit_not_a_number_rw() {
        let response = get_projects_limit_not_a_number(true).await;
        assert_limit_out_of_range(response).await;
    }

    #[tokio::test]
    async fn get_projects_limit_not_a_number_ro() {
        let response = get_projects_limit_not_a_number(false).await;
        assert_limit_out_of_range(response).await;
    }

    async fn get_projects_seek_start_ok(rw: bool) -> Response {
        let query = SeekLink::new(
            &Seek {
                anchor: Anchor::Start,
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                facets: vec![]
            },
            None
        );

        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects{query}"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_projects_seek_start_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_seek_start_ok_rw() {
        let response = get_projects_seek_start_ok(true).await;
        assert_projects_seek_start_ok(response).await;
    }

    #[tokio::test]
    async fn get_projects_seek_start_ok_ro() {
        let response = get_projects_seek_start_ok(false).await;
        assert_projects_seek_start_ok(response).await;
    }

    async fn get_projects_seek_end_ok(rw: bool) -> Response {
        let query = SeekLink::new(
            &Seek {
                anchor: Anchor::Start,
                sort_by: SortBy::ProjectName,
                dir: Direction::Descending,
                facets: vec![]
            },
            None
        );

        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects{query}"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_projects_seek_end_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_seek_end_ok_rw() {
        let response = get_projects_seek_end_ok(true).await;
        assert_projects_seek_end_ok(response).await;
    }

    #[tokio::test]
    async fn get_projects_seek_end_ok_ro() {
        let response = get_projects_seek_end_ok(false).await;
        assert_projects_seek_end_ok(response).await;
    }

    async fn get_projects_seek_before_ok(rw: bool) -> Response {
        let query = SeekLink::new(
            &Seek {
                anchor: Anchor::Before("xyz".into(), 0),
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                facets: vec![]
            },
            None
        );

        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects{query}"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_projects_seek_before_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_seek_before_ok_rw() {
        let response = get_projects_seek_before_ok(true).await;
        assert_projects_seek_before_ok(response).await;
    }

    #[tokio::test]
    async fn get_projects_seek_before_ok_ro() {
        let response = get_projects_seek_before_ok(false).await;
        assert_projects_seek_before_ok(response).await;
    }

    async fn get_projects_seek_after_ok(rw: bool) -> Response {
        let query = SeekLink::new(
            &Seek {
                anchor: Anchor::After("xyz".into(), 0),
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                facets: vec![]
            },
            None
        );

        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects{query}"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_projects_seek_after_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            None
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_seek_after_ok_rw() {
        let response = get_projects_seek_after_ok(true).await;
        assert_projects_seek_after_ok(response).await;
    }

    #[tokio::test]
    async fn get_projects_seek_after_ok_ro() {
        let response = get_projects_seek_after_ok(false).await;
        assert_projects_seek_after_ok(response).await;
    }

    async fn get_projects_seek_and_limit_ok(rw: bool) -> Response {
        let query = SeekLink::new(
            &Seek {
                anchor: Anchor::Start,
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                facets: vec![]
            },
            Limit::new(5)
        );

        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects{query}"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_projects_seek_and_limit_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            Limit::new(5)
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            Limit::new(5)
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_seek_and_limit_ok_rw() {
        let response = get_projects_seek_and_limit_ok(true).await;
        assert_projects_seek_and_limit_ok(response).await;
     }

    #[tokio::test]
    async fn get_projects_seek_and_limit_ok_ro() {
        let response = get_projects_seek_and_limit_ok(false).await;
        assert_projects_seek_and_limit_ok(response).await;
    }

    async fn get_projects_limit_and_seek_ok(rw: bool) -> Response {
        let query = SeekLink::new(
            &Seek {
                anchor: Anchor::Start,
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                facets: vec![]
            },
            Limit::new(5)
        );

        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects{query}"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_projects_limit_and_seek(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Projects>(response).await,
            Projects {
                projects: vec![
                    PROJECT_SUMMARY_A.clone(),
                    PROJECT_SUMMARY_B.clone()
                ],
                meta: Pagination {
                    prev_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::Before("project_a".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            Limit::new(5)
                        )
                    ),
                    next_page: Some(
                        SeekLink::new(
                            &Seek {
                                anchor: Anchor::After("project_b".into(), 0),
                                sort_by: SortBy::ProjectName,
                                dir: Direction::Ascending,
                                facets: vec![]
                            },
                            Limit::new(5)
                        )
                    ),
                    total: 1234
                }
            }
        );
    }

    #[tokio::test]
    async fn get_projects_limit_and_seek_ok_rw() {
        let response = get_projects_limit_and_seek_ok(true).await;
        assert_projects_limit_and_seek(response).await;
    }

    #[tokio::test]
    async fn get_projects_limit_and_seek_ok_ro() {
        let response = get_projects_limit_and_seek_ok(false).await;
        assert_projects_limit_and_seek(response).await;
    }

    async fn get_project_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/a_project"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_project_data_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<ProjectData>(response).await,
            *EIA_PROJECT_DATA
        );
    }

    #[tokio::test]
    async fn get_project_ok_rw() {
        let response = get_project_ok(true).await;
        assert_project_data_ok(response).await;
    }

    #[tokio::test]
    async fn get_project_ok_ro() {
        let response = get_project_ok(false).await;
        assert_project_data_ok(response).await;
    }

    async fn get_project_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/not_a_project"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_project_not_a_project_rw() {
        let response = get_project_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_project_not_a_project_ro() {
        let response = get_project_not_a_project(false).await;
        assert_not_found(response).await;
    }

    async fn post_project_ok(rw: bool) -> Response {
        let proj_data = ProjectDataPost {
            name: "name".into(),
            description: "A module for Empires in Arms".into(),
            tags: vec![],
            game: GameDataPost {
                title: "Empires in Arms".into(),
                title_sort_key: "Empires in Arms".into(),
                publisher: "Avalon Hill".into(),
                year: "1983".into(),
                players: RangePost::default(),
                length: RangePost::default()
            },
            readme: "".into(),
            image: None
        };

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&proj_data).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_project_ok_rw() {
        let response = post_project_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn post_project_ok_ro() {
        let response = post_project_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn post_project_unauth(rw: bool) -> Response {
        let proj_data = ProjectDataPost {
            name: "name".into(),
            description: "A module for Empires in Arms".into(),
            tags: vec![],
            game: GameDataPost {
                title: "Empires in Arms".into(),
                title_sort_key: "Empires in Arms".into(),
                publisher: "Avalon Hill".into(),
                year: "1983".into(),
                players: RangePost::default(),
                length: RangePost::default()
            },
            readme: "".into(),
            image: None
        };

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&proj_data).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_project_unauth_rw() {
        let response = post_project_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn post_project_unauth_ro() {
        let response = post_project_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn post_project_wrong_json(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from(r#"{ "garbage": "whatever" }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_project_wrong_json_rw() {
        let response = post_project_wrong_json(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn post_project_wrong_json_ro() {
        let response = post_project_wrong_json(false).await;
        assert_forbidden(response).await;
    }

    async fn post_project_wrong_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project"))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_project_wrong_mime_type_rw() {
        let response = post_project_wrong_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_project_wrong_mime_type_ro() {
        let response = post_project_wrong_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn post_project_no_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_project_no_mime_type_rw() {
        let response = post_project_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_project_no_mime_type_ro() {
        let response = post_project_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_ok(rw: bool) -> Response {
        let proj_data = ProjectDataPatch {
            description: Some("A module for Empires in Arms".into()),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&proj_data).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_ok_rw() {
        let response = patch_project_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn patch_project_ok_ro() {
        let response = patch_project_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_clear_image_ok(rw: bool) -> Response {
        let proj_data = ProjectDataPatch {
            image: Some(None),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&proj_data).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_clear_image_ok_rw() {
        let response = patch_project_clear_image_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn patch_project_clear_image_ok_ro() {
        let response = patch_project_clear_image_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_no_data(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from("{}"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_no_data_rw() {
        let response = patch_project_no_data(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn patch_project_no_data_ro() {
        let response = patch_project_no_data(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_unauth(rw: bool) -> Response {
        let proj_data = ProjectDataPatch {
            description: Some("A module for Empires in Arms".into()),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&proj_data).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_unauth_rw() {
        let response = patch_project_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn patch_project_unauth_ro() {
        let response = patch_project_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_not_owner(rw: bool) -> Response {
        let proj_data = ProjectDataPatch {
            description: Some("A module for Empires in Arms".into()),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(AUTHORIZATION, token(0))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&proj_data).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_not_owner_rw() {
        let response = patch_project_not_owner(true).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn patch_project_not_owner_ro() {
        let response = patch_project_not_owner(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_wrong_json(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from(r#"{ "garbage": "whatever" }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_wrong_json_rw() {
        let response = patch_project_wrong_json(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn patch_project_wrong_json_ro() {
        let response = patch_project_wrong_json(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_wrong_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_wrong_mime_type_rw() {
        let response = patch_project_wrong_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn patch_project_wrong_mime_type_ro() {
        let response = patch_project_wrong_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_project_no_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_project_no_mime_type_rw() {
        let response = patch_project_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn patch_project_no_mime_type_ro() {
        let response = patch_project_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn get_project_revision_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/a_project/1"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_project_revision_ok_rw() {
        let response = get_project_revision_ok(true).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<ProjectData>(response).await,
            *EIA_PROJECT_DATA
        );
    }

    #[tokio::test]
    async fn get_project_revision_ok_ro() {
        let response = get_project_revision_ok(false).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<ProjectData>(response).await,
            *EIA_PROJECT_DATA
        );
    }

    async fn get_project_revision_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/not_a_project/1"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_project_revision_not_a_project_rw() {
        let response = get_project_revision_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_project_revision_not_a_project_ro() {
        let response = get_project_revision_not_a_project(false).await;
        assert_not_found(response).await;
    }

    async fn get_project_revision_not_a_revision(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/a_project/2"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_project_revision_not_a_revision_rw() {
        let response = get_project_revision_not_a_revision(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_project_revision_not_a_revision_ro() {
        let response = get_project_revision_not_a_revision(false).await;
        assert_not_found(response).await;
    }

    async fn get_owners_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_owners_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Users>(response).await,
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into()
                ]
            }
        );
    }

    #[tokio::test]
    async fn get_owners_ok_rw() {
        let response = get_owners_ok(true).await;
        assert_owners_ok(response).await;
    }

    #[tokio::test]
    async fn get_owners_ok_ro() {
        let response = get_owners_ok(false).await;
        assert_owners_ok(response).await;
    }

    async fn get_owners_bad_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/not_a_project/owners"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_owners_bad_project_rw() {
        let response = get_owners_bad_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_owners_bad_project_ro() {
        let response = get_owners_bad_project(false).await;
        assert_not_found(response).await;
    }

    async fn put_owners_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_owners_ok_rw() {
        let response = put_owners_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn put_owners_ok_ro() {
        let response = put_owners_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn put_owners_bad_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/not_a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_owners_bad_project_rw() {
        let response =  put_owners_bad_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn put_owners_bad_project_ro() {
        let response =  put_owners_bad_project(false).await;
        assert_forbidden(response).await;
    }

    async fn put_owners_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_owners_unauth_rw() {
        let response =  put_owners_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn put_owners_unauth_ro() {
        let response =  put_owners_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn put_owners_not_owner(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(0))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_owners_not_owner_rw() {
        let response =  put_owners_not_owner(true).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn put_owners_not_owner_ro() {
        let response =  put_owners_not_owner(false).await;
        assert_forbidden(response).await;
    }

    async fn put_owners_wrong_json(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "garbage": "whatever" }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_owners_wrong_json_rw() {
        let response =  put_owners_wrong_json(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn put_owners_wrong_json_ro() {
        let response =  put_owners_wrong_json(false).await;
        assert_forbidden(response).await;
    }

    async fn put_owners_wrong_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_owners_wrong_mime_type_rw() {
        let response =  put_owners_wrong_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn put_owners_wrong_mime_type_ro() {
        let response =  put_owners_wrong_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn put_owners_no_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_owners_no_mime_type_rw() {
        let response =  put_owners_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn put_owners_no_mime_type_ro() {
        let response =  put_owners_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_owners_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_owners_ok_rw() {
        let response = delete_owners_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn delete_owners_ok_ro() {
        let response = delete_owners_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_owners_bad_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/not_a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_owners_bad_project_rw() {
        let response = delete_owners_bad_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn delete_owners_bad_project_ro() {
        let response = delete_owners_bad_project(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_owners_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_owners_unauth_rw() {
        let response = delete_owners_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn delete_owners_unauth_ro() {
        let response = delete_owners_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_owners_not_owner(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(0))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_owners_not_owner_rw() {
        let response = delete_owners_not_owner(true).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn delete_owners_not_owner_ro() {
        let response = delete_owners_not_owner(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_owners_wrong_json(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(r#"{ "garbage": "whatever" }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_owners_wrong_json_rw() {
        let response = delete_owners_wrong_json(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn delete_owners_wrong_json_ro() {
        let response = delete_owners_wrong_json(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_owners_wrong_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_owners_wrong_mime_type_rw() {
        let response = delete_owners_wrong_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn delete_owners_wrong_mime_type_ro() {
        let response = delete_owners_wrong_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_owners_no_mime_type(rw: bool) -> Response {
         try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from(r#"{ "users": ["alice", "bob"] }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_owners_no_mime_type_rw() {
        let response = delete_owners_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn delete_owners_no_mime_type_ro() {
        let response = delete_owners_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn get_players_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/a_project/players"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_players_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Users>(response).await,
            Users {
                users: vec![
                    "player 1".into(),
                    "player 2".into()
                ]
            }
        );
    }

    #[tokio::test]
    async fn get_players_ok_rw() {
        let response = get_players_ok(true).await;
        assert_players_ok(response) .await;
    }

    #[tokio::test]
    async fn get_players_ok_ro() {
        let response = get_players_ok(false).await;
        assert_players_ok(response) .await;
    }

    async fn get_players_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/not_a_project/players"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_players_not_a_project_rw() {
        let response = get_players_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_players_not_a_project_ro() {
        let response = get_players_not_a_project(true).await;
        assert_not_found(response).await;
    }

    async fn put_players_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/players"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_players_ok_rw() {
        let response = put_players_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn put_players_ok_ro() {
        let response = put_players_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn put_players_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/not_a_project/owners"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_players_not_a_project_rw() {
        let response = put_players_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn put_players_not_a_project_ro() {
        let response = put_players_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn put_players_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{API_V1}/projects/a_project/players"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn put_players_unauth_rw() {
        let response = put_players_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn put_players_unauth_ro() {
        let response = put_players_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_players_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/players"))
                .header(AUTHORIZATION, token(8))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_players_ok_rw() {
        let response = delete_players_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn delete_players_ok_ro() {
        let response = delete_players_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_players_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/not_a_project/players"))
                .header(AUTHORIZATION, token(8))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_players_not_a_project_rw() {
        let response = delete_players_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn delete_players_not_a_project_ro() {
        let response = delete_players_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_players_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/players"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_players_unauth_rw() {
        let response = delete_players_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn delete_players_unauth_ro() {
        let response = delete_players_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn get_image_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_image_ok_rw() {
        let response = get_image_ok(true).await;

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(LOCATION).unwrap(),
            "https://example.com/img.png"
        );
    }

    #[tokio::test]
    async fn get_image_ok_ro() {
        let response = get_image_ok(true).await;

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(LOCATION).unwrap(),
            "https://example.com/img.png"
        );
    }

    async fn get_image_not_a_project(rw: bool) -> Response {
         try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/not_a_project/images/img.png"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_image_not_a_project_rw() {
        let response = get_image_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_image_not_a_project_ro() {
        let response = get_image_not_a_project(false).await;
        assert_not_found(response).await;
    }

    async fn get_image_not_an_image(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/projects/a_project/images/not_a.png"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_image_not_an_image_rw() {
        let response = get_image_not_an_image(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn get_image_not_an_image_ro() {
        let response = get_image_not_an_image(false).await;
        assert_not_found(response).await;
    }

    async fn post_image_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_ok_rw() {
        let response = post_image_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn post_image_ok_ro() {
        let response = post_image_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_not_a_project_rw() {
        let response = post_image_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_image_not_a_project_ro() {
        let response = post_image_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_unauth_rw() {
        let response = post_image_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn post_image_unauth_ro() {
        let response = post_image_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_not_owner(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(0))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_not_owner_rw() {
        let response = post_image_not_owner(true).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn post_image_not_owner_ro() {
        let response = post_image_not_owner(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_no_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_no_mime_type_rw() {
        let response = post_image_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_image_no_mime_type_ro() {
        let response = post_image_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_bad_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(CONTENT_LENGTH, 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_bad_mime_type_rw() {
        let response = post_image_bad_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_image_bad_mime_type_ro() {
        let response = post_image_bad_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_too_large(rw: bool) -> Response {
        let long = "x".repeat(MAX_IMAGE_SIZE + 1);

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .header(CONTENT_LENGTH, long.len())
                .body(Body::from(long))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_too_large_rw() {
        let response = post_image_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_image_too_large_ro() {
        let response = post_image_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_too_large_no_content_length(rw: bool) -> Response {
        let long = "x".repeat(MAX_IMAGE_SIZE + 1);

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::from(long))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_too_large_no_content_length_rw() {
        let response = post_image_too_large_no_content_length(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_image_too_large_no_content_length_ro() {
        let response = post_image_too_large_no_content_length(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_content_length_too_large(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .header(CONTENT_LENGTH, MAX_IMAGE_SIZE + 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_content_length_too_large_rw() {
        let response = post_image_content_length_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_image_content_length_too_large_ro() {
        let response = post_image_content_length_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_image_content_length_way_too_large(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/images/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .header(CONTENT_LENGTH, u64::MAX)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_image_content_length_way_too_large_rw() {
        let response = post_image_content_length_way_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_image_content_length_way_too_large_ro() {
        let response = post_image_content_length_way_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(CONTENT_LENGTH, 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    async fn post_gallery_image_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_ok_rw() {
        let response = post_gallery_image_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_ok_ro() {
        let response = post_gallery_image_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_not_a_project_rw() {
        let response = post_gallery_image_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_not_a_project_ro() {
        let response = post_gallery_image_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_unauth_rw() {
        let response = post_gallery_image_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_unauth_ro() {
        let response = post_gallery_image_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_not_owner(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(0))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_not_owner_rw() {
        let response = post_gallery_image_not_owner(true).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_not_owner_ro() {
        let response = post_gallery_image_not_owner(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_no_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_no_mime_type_rw() {
        let response = post_gallery_image_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_no_mime_type_ro() {
        let response = post_gallery_image_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_bad_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(CONTENT_LENGTH, 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_bad_mime_type_rw() {
        let response = post_gallery_image_bad_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_bad_mime_type_ro() {
        let response = post_gallery_image_bad_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_too_large(rw: bool) -> Response {
        let long = "x".repeat(MAX_IMAGE_SIZE + 1);

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .header(CONTENT_LENGTH, long.len())
                .body(Body::from(long))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_too_large_rw() {
        let response = post_gallery_image_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_too_large_ro() {
        let response = post_gallery_image_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_too_large_no_content_length(rw: bool) -> Response {
        let long = "x".repeat(MAX_IMAGE_SIZE + 1);

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::from(long))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_too_large_no_content_length_rw() {
        let response = post_gallery_image_too_large_no_content_length(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_too_large_no_content_length_ro() {
        let response = post_gallery_image_too_large_no_content_length(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_content_length_too_large(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .header(CONTENT_LENGTH, MAX_IMAGE_SIZE + 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_content_length_too_large_rw() {
        let response = post_gallery_image_content_length_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_content_length_too_large_ro() {
        let response = post_gallery_image_content_length_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_gallery_image_content_length_way_too_large(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/gallery/img.png"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .header(CONTENT_LENGTH, u64::MAX)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_gallery_image_content_length_way_too_large_rw() {
        let response = post_gallery_image_content_length_way_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_gallery_image_content_length_way_too_large_ro() {
        let response = post_gallery_image_content_length_way_too_large(false).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn post_file_ok_rw() {
        let response = post_file_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn post_file_ok_ro() {
        let response = post_file_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_not_a_project_rw() {
        let response = post_file_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_file_not_a_project_ro() {
        let response = post_file_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_not_a_package(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/not_a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_not_a_package_rw() {
        let response = post_file_not_a_package(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_file_not_a_package_ro() {
        let response = post_file_not_a_package(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_not_a_release(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/bogus/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, IMAGE_PNG.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_not_a_release_rw() {
        let response = post_file_not_a_release(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_file_not_a_release_ro() {
        let response = post_file_not_a_release(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_unauth_rw() {
        let response = post_file_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn post_file_unauth_ro() {
        let response = post_file_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_not_owner(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(0))
                .header(CONTENT_LENGTH, 1)
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_not_owner_rw() {
        let response = post_file_not_owner(true).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn post_file_not_owner_ro() {
        let response = post_file_not_owner(true).await;
        assert_forbidden(response).await;
    }

    async fn post_file_too_large(rw: bool) -> Response {
        let long = "x".repeat(MAX_FILE_SIZE + 1);

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(CONTENT_LENGTH, long.len())
                .body(Body::from(long))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_too_large_rw() {
        let response = post_file_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_file_too_large_ro() {
        let response = post_file_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_too_large_no_content_length(rw: bool) -> Response {
        let long = "x".repeat(MAX_FILE_SIZE + 1);

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .body(Body::from(long))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_too_large_no_content_length_rw() {
        let response = post_file_too_large_no_content_length(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_file_too_large_no_content_length_ro() {
        let response = post_file_too_large_no_content_length(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_content_length_too_large(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(CONTENT_LENGTH, MAX_FILE_SIZE + 1)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_content_length_too_large_rw() {
        let response = post_file_content_length_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_file_content_length_too_large_ro() {
        let response = post_file_content_length_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_content_length_way_too_large(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(CONTENT_LENGTH, u64::MAX)
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_file_content_length_way_too_large_rw() {
        let response = post_file_content_length_way_too_large(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_file_content_length_way_too_large_ro() {
        let response = post_file_content_length_way_too_large(false).await;
        assert_forbidden(response).await;
    }

    async fn post_file_payload_exceeds_content_length(rw: bool) -> Response {
        let long = "x".repeat(MAX_FILE_SIZE);

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3/war_and_peace.txt"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(CONTENT_LENGTH, long.len() - 1)
                .body(Body::from(long))
                .unwrap(),
            rw
        ).await
    }

    #[tokio::test]
    async fn post_file_payload_exceeds_content_length_rw() {
        let response = post_file_payload_exceeds_content_length(true).await;
        assert_payload_too_large(response).await;
    }

    #[tokio::test]
    async fn post_file_payload_exceeds_content_length_ro() {
        let response = post_file_payload_exceeds_content_length(false).await;
        assert_forbidden(response).await;
    }

    async fn post_package_ok(rw: bool) -> Response {
        let pd = PackageDataPost {
            name: "pkg".into(),
            sort_key: 4,
            description: "".into()
        };

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/newpkg"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_package_ok_rw() {
        let response = post_package_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn post_package_ok_ro() {
        let response = post_package_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn post_package_not_a_project(rw: bool) -> Response {
        let pd = PackageDataPost {
            name: "pkg".into(),
            sort_key: 4,
            description: "".into()
        };

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project/packages/a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_package_not_a_project_rw() {
        let response = post_package_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_package_not_a_project_ro() {
        let response = post_package_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn post_package_unauth(rw: bool) -> Response {
        let pd = PackageDataPost {
            name: "pkg".into(),
            sort_key: 4,
            description: "".into()
        };

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_package_unauth_rw() {
        let response = post_package_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn post_package_unauth_ro() {
        let response = post_package_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn post_package_already_exists(rw: bool) -> Response {
        let pd = PackageDataPost {
            name: "pkg".into(),
            sort_key: 4,
            description: "".into()
        };

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_package_already_exists_rw() {
        let response = post_package_already_exists(true).await;
        assert_malformed_query(response).await;
    }

    #[tokio::test]
    async fn post_package_already_exists_ro() {
        let response = post_package_already_exists(false).await;
        assert_forbidden(response).await;
    }

    async fn update_package_ok(rw: bool) -> Response {
        let pd = PackageDataPatch {
            sort_key: Some(4),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn update_package_ok_rw() {
        let response = update_package_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn update_package_ok_ro() {
        let response = update_package_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn update_package_not_a_project(rw: bool) -> Response {
        let pd = PackageDataPatch {
            sort_key: Some(4),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/not_a_project/packages/a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn update_package_not_a_project_rw() {
        let response = update_package_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn update_package_not_a_project_ro() {
        let response = update_package_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn update_package_not_a_package(rw: bool) -> Response {
        let pd = PackageDataPatch {
            sort_key: Some(4),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/packages/not_a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn update_package_not_a_package_rw() {
        let response = update_package_not_a_package(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn update_package_not_a_packaget_ro() {
        let response = update_package_not_a_package(false).await;
        assert_forbidden(response).await;
    }

    async fn update_package_unauth(rw: bool) -> Response {
        let pd = PackageDataPatch {
            sort_key: Some(4),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&pd).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn update_package_unauth_rw() {
        let response = update_package_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn update_package_unauth_ro() {
        let response = update_package_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_package_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_package_ok_rw() {
        let response = delete_package_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn delete_package_ok_ro() {
        let response = delete_package_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_package_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/not_a_project/packages/a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_package_not_a_project_rw() {
        let response = delete_package_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn delete_package_not_a_project_ro() {
        let response = delete_package_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_package_not_a_package(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/not_a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_package_not_a_package_rw() {
        let response = delete_package_not_a_package(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn delete_package_not_a_packaget_ro() {
        let response = delete_package_not_a_package(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_package_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_package_unauth_rw() {
        let response = delete_package_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn delete_package_unauth_ro() {
        let response = delete_package_unauth(false).await;
        assert_forbidden(response).await;
    }


    async fn delete_package_not_empty(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/nonempty"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_package_not_empty_rw() {
        let response = delete_package_not_empty(true).await;
        assert_malformed_query(response).await;
    }

    #[tokio::test]
    async fn delete_package_not_empty_ro() {
        let response = delete_package_not_empty(false).await;
        assert_forbidden(response).await;
    }

    async fn post_release_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.4"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_release_ok_rw() {
        let response = post_release_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn post_release_ok_ro() {
        let response = post_release_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn post_release_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project/packages/a_package/1.2.4"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_release_not_a_project_rw() {
        let response = post_release_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_release_not_a_project_ro() {
        let response = post_release_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn post_release_not_a_package(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/not_a_package/1.2.4"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_release_not_a_package_rw() {
        let response = post_release_not_a_package(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_release_not_a_package_ro() {
        let response = post_release_not_a_package(false).await;
        assert_forbidden(response).await;
    }

    async fn post_release_already_exists(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_release_already_exists_rw() {
        let response = post_release_already_exists(true).await;
        assert_malformed_query(response).await;
    }

    #[tokio::test]
    async fn post_release_already_exists_ro() {
        let response = post_release_already_exists(false).await;
        assert_forbidden(response).await;
    }

    async fn post_release_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.4"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_release_unauth_rw() {
        let response = post_release_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn post_release_unauth_ro() {
        let response = post_release_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_release_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_release_ok_rw() {
        let response = delete_release_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn delete_release_ok_ro() {
        let response = delete_release_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_release_not_a_project(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/not_a_project/packages/a_package/1.2.3"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_release_not_a_project_rw() {
        let response = delete_release_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn delete_release_not_a_project_ro() {
        let response = delete_release_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_release_not_a_package(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/not_a_package"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_release_not_a_package_rw() {
        let response = delete_release_not_a_package(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn delete_release_not_a_packaget_ro() {
        let response = delete_release_not_a_package(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_release_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.3"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_release_unauth_rw() {
        let response = delete_release_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn delete_release_unauth_ro() {
        let response = delete_release_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn delete_release_not_empty(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("{API_V1}/projects/a_project/packages/a_package/1.2.4"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn delete_release_not_empty_rw() {
        let response = delete_release_not_empty(true).await;
        assert_malformed_query(response).await;
    }

    #[tokio::test]
    async fn delete_release_not_empty_ro() {
        let response = delete_release_not_empty(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_ok(rw: bool) -> Response {
        let gallery_patch = GalleryPatch {
            ops: vec![GalleryOp::Update { id: 0, description: "".into() }]
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&gallery_patch).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_ok_rw() {
        let response = patch_gallery_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_ok_ro() {
        let response = patch_gallery_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_empty_ops(rw: bool) -> Response {
        let gallery_patch = GalleryPatch {
            ops: vec![]
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&gallery_patch).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_empty_ops_rw() {
        let response = patch_gallery_empty_ops(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_empty_ops_ro() {
        let response = patch_gallery_empty_ops(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_no_data(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from("{}"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_no_data_rw() {
        let response = patch_gallery_no_data(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_no_data_ro() {
        let response = patch_gallery_no_data(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_unauth(rw: bool) -> Response {
        let proj_data = ProjectDataPatch {
            description: Some("A module for Empires in Arms".into()),
            ..Default::default()
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&proj_data).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_unauth_rw() {
        let response = patch_gallery_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_unauth_ro() {
        let response = patch_gallery_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_not_owner(rw: bool) -> Response {
        let gallery_patch = GalleryPatch {
            ops: vec![GalleryOp::Update { id: 0, description: "".into() }]
        };

        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(AUTHORIZATION, token(0))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&gallery_patch).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_not_owner_rw() {
        let response = patch_gallery_not_owner(true).await;
        assert_forbidden(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_not_owner_ro() {
        let response = patch_gallery_not_owner(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_wrong_json(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from(r#"{ "garbage": "whatever" }"#))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_wrong_json_rw() {
        let response = patch_gallery_wrong_json(true).await;
        assert_unprocessable_entity(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_wrong_json_ro() {
        let response = patch_gallery_wrong_json(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_wrong_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_wrong_mime_type_rw() {
        let response = patch_gallery_wrong_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_wrong_mime_type_ro() {
        let response = patch_gallery_wrong_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_gallery_no_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/projects/a_project/gallery"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_gallery_no_mime_type_rw() {
        let response = patch_gallery_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn patch_gallery_no_mime_type_ro() {
        let response = patch_gallery_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn post_flag_ok(rw: bool) -> Response {
        let fp = FlagPost::Spam;

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/flag"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&fp).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_flag_ok_rw() {
        let response = post_flag_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn post_flag_ok_ro() {
        let response = post_flag_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn post_flag_not_a_project(rw: bool) -> Response {
        let fp = FlagPost::Spam;

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/not_a_project/flag"))
                .header(AUTHORIZATION, token(BOB_UID))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&fp).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_flag_not_a_project_rw() {
        let response = post_flag_not_a_project(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn post_flag_not_a_project_ro() {
        let response = post_flag_not_a_project(false).await;
        assert_forbidden(response).await;
    }

    async fn post_flag_unauth(rw: bool) -> Response {
        let fp = FlagPost::Spam;

        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/flag"))
                .header(CONTENT_TYPE, APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&fp).unwrap()))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_flag_unauth_rw() {
        let response = post_flag_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn post_flag_unauth_ro() {
        let response = post_flag_unauth(false).await;
        assert_forbidden(response).await;
    }

    async fn post_flag_wrong_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/flag"))
                .header(CONTENT_TYPE, TEXT_PLAIN.as_ref())
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_flag_wrong_mime_type_rw() {
        let response = post_flag_wrong_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_flag_wrong_mime_type_ro() {
        let response = post_flag_wrong_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn post_flag_no_mime_type(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{API_V1}/projects/a_project/flag"))
                .header(AUTHORIZATION, token(BOB_UID))
                .body(Body::from("stuff"))
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn post_flag_no_mime_type_rw() {
        let response = post_flag_no_mime_type(true).await;
        assert_unsupported_media_type(response).await;
    }

    #[tokio::test]
    async fn post_flag_no_mime_type_ro() {
        let response = post_flag_no_mime_type(false).await;
        assert_forbidden(response).await;
    }

    async fn get_admin_flags_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/admin/flags"))
                .header(AUTHORIZATION, token(5))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[track_caller]
    async fn assert_flags_ok(response: Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_as::<Flags>(response).await,
            Flags {
                flags: vec![
                    FlagData {
                        flag_id: 1,
                        project: "a_project".into(),
                        slug: "a_project".into(),
                        flagged_by: "bob".into(),
                        flagged_at: "2023-10-30T18:53:53.056386142Z".into(),
                        flag: FlagTag::Spam,
                        message: None
                    }
                ]
            }
        );
    }

    #[tokio::test]
    async fn get_admin_flags_ok_rw() {
        let response = get_admin_flags_ok(true).await;
        assert_flags_ok(response).await;
    }

    #[tokio::test]
    async fn get_admin_flags_ok_ro() {
        let response = get_admin_flags_ok(false).await;
        assert_flags_ok(response).await;
    }

    async fn get_admin_flags_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{API_V1}/admin/flags"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn get_admin_flags_unauth_rw() {
        let response = get_admin_flags_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn get_admin_flags_unauth_ro() {
        let response = get_admin_flags_unauth(false).await;
        assert_unauthorized(response).await;
    }

    async fn patch_admin_flags_ok(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/admin/flags/1"))
                .header(AUTHORIZATION, token(5))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_admin_flags_ok_rw() {
        let response = patch_admin_flags_ok(true).await;
        assert_ok(response).await;
    }

    #[tokio::test]
    async fn patch_admin_flags_ok_ro() {
        let response = patch_admin_flags_ok(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_admin_flags_not_a_flag(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/admin/flags/2"))
                .header(AUTHORIZATION, token(5))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_admin_flags_not_a_flag_rw() {
        let response = patch_admin_flags_not_a_flag(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn patch_admin_flags_not_a_flag_ro() {
        let response = patch_admin_flags_not_a_flag(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_admin_flags_bad_id(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/admin/flags/not_an_id"))
                .header(AUTHORIZATION, token(5))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_admin_flags_bad_id_rw() {
        let response = patch_admin_flags_bad_id(true).await;
        assert_not_found(response).await;
    }

    #[tokio::test]
    async fn patch_admin_flags_bad_id_ro() {
        let response = patch_admin_flags_bad_id(false).await;
        assert_forbidden(response).await;
    }

    async fn patch_admin_flags_unauth(rw: bool) -> Response {
        try_request(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{API_V1}/admin/flags/1"))
                .body(Body::empty())
                .unwrap(),
            rw
        )
        .await
    }

    #[tokio::test]
    async fn patch_admin_flags_unauth_rw() {
        let response = patch_admin_flags_unauth(true).await;
        assert_unauthorized(response).await;
    }

    #[tokio::test]
    async fn patch_admin_flags_unauth_ro() {
        let response = patch_admin_flags_unauth(false).await;
        assert_forbidden(response).await;
    }
}
