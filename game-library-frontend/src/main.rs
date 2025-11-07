use askama::Template;
use axum::{
    Router, serve,
    body::Body,
    http::StatusCode,
    extract::{ConnectInfo, Path, Request, State},
    response::{Html, IntoResponse, Response},
    routing::get
};
use bytes::{Buf, Bytes};
use futures::future;
use http::header::{CACHE_CONTROL, HeaderValue};
use mime::APPLICATION_JSON;
use reqwest::{
    Client,
    header::ACCEPT
};
use serde::{
    Deserialize, Serialize,
    de::DeserializeOwned
};
use std::{
    fs,
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration
};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    services::{ServeDir, ServeFile},
    set_header::SetResponseHeaderLayer,
    timeout::TimeoutLayer,
    trace::{DefaultOnFailure, DefaultOnResponse, MakeSpan, TraceLayer}
};
use tracing::{error, info, info_span, Level, Span};
use tracing_panic::panic_hook;
use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::SubscriberInitExt
};

const SITE_DIR: &str = "../../../site";
const DIST_DIR: &str = "app/dist";

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

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("{0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("{0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("{0}")]
    Io(#[from] io::Error)
}

fn routes(base_path: &str, log_headers: bool) -> Router<Arc<AppState>> {
    Router::new()
        .route_service(
            if base_path.is_empty() { "/" } else { base_path },
            ServeFile::new(format!("{DIST_DIR}/root.html"))
        )
        .route_service(
            &format!("{base_path}/projects"),
            ServeFile::new(format!("{DIST_DIR}/projects.html"))
        )
        .route(
            &format!("{base_path}/projects/{{project}}"),
            get(project_page)
        )
        .route_service(
            &format!("{base_path}/publishers"),
            ServeFile::new(format!("{DIST_DIR}/publishers.html"))
        )
        .route_service(
            &format!("{base_path}/tags"),
            ServeFile::new(format!("{DIST_DIR}/tags.html"))
        )
        .route_service(
            &format!("{base_path}/search"),
            ServeFile::new(format!("{DIST_DIR}/search.html"))
        )
        .route_service(
            &format!("{base_path}/new"),
            ServeFile::new(format!("{DIST_DIR}/new.html"))
        )
        .route_service(
            &format!("{base_path}/admin/flags"),
            ServeFile::new(format!("{DIST_DIR}/flags.html"))
        )
        .nest_service(
            &format!("{base_path}/assets/"),
            ServiceBuilder::new()
                .layer(SetResponseHeaderLayer::overriding(
                    CACHE_CONTROL,
                    HeaderValue::from_static("public, max-age=31536000, immutable")
                ))
                .service(ServeDir::new(format!("{DIST_DIR}/assets")))
        )
        .nest_service(
            &format!("{base_path}/"),
            ServeDir::new(DIST_DIR)
        )
        .fallback_service(ServeDir::new(SITE_DIR))
        .layer(
            ServiceBuilder::new()
                .layer(CompressionLayer::new())
                 // ensure requests don't block shutdown
                .layer(TimeoutLayer::new(Duration::from_secs(10)))
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(SpanMaker::new().include_headers(log_headers))
                .on_response(DefaultOnResponse::new().level(Level::INFO))
                .on_failure(DefaultOnFailure::new().level(Level::WARN))
        )
}

#[derive(Clone)]
pub struct AppState {
    client: Client,
    api_url: String
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("{0}")]
    BackendError(#[from] reqwest::Error),
    #[error("{0}")]
    JsonError(#[from] serde_json::Error)
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("{}", self);
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Range {
    pub min: Option<i64>,
    pub max: Option<i64>
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GameData {
    pub title: String,
    pub title_sort_key: String,
    pub publisher: String,
    pub year: String,
    pub players: Range,
    pub length: Range
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileData {
    pub filename: String,
    pub url: String,
    pub size: i64,
    pub sha256: String,
    pub published_at: String,
    pub published_by: String,
    pub requires: Option<String>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReleaseData {
    pub version: String,
    pub files: Vec<FileData>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageData {
    pub name: String,
    pub slug: String,
    pub sort_key: i64,
    pub description: String,
    pub releases: Vec<ReleaseData>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GalleryImage {
    pub id: i64,
    pub filename: String,
    pub description: String
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectData {
    pub name: String,
    pub slug: String,
    pub description: String,
    pub revision: i64,
    pub created_at: String,
    pub modified_at: String,
    pub tags: Vec<String>,
    pub game: GameData,
    pub readme: String,
    pub image: Option<String>,
    pub owners: Vec<String>,
    pub packages: Vec<PackageData>,
    pub gallery: Vec<GalleryImage>
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Users {
    pub users: Vec<String>
}

#[derive(Template)]
#[template(path = "project.html")]
struct ProjectTemplate(
    Result<ProjectData, AppError>,
    Result<Users, AppError>
);

struct HtmlTemplate<T>(T);

impl<T: Template> IntoResponse for HtmlTemplate<T> {
    fn into_response(self) -> Response {
        match self.0.render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to render template. Error: {}", err),
            ).into_response(),
        }
    }
}

async fn get_data(
    client: &Client,
    url: &str
) -> Result<Bytes, AppError>
{
    info!("getting {url}");
    Ok(
        client.get(url)
            .header(ACCEPT, APPLICATION_JSON.as_ref())
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?
    )
}

fn parse_data<T>(
    bytes: Bytes
) -> Result<T, AppError>
where
    T: DeserializeOwned
{
    Ok(serde_json::from_reader(bytes.reader())?)
}

async fn project_page(
    Path(project): Path<String>,
    State(state): State<Arc<AppState>>
) -> HtmlTemplate<ProjectTemplate>
{
    let project = urlencoding::encode(&project);

    let players_url = format!("{}/projects/{project}/players", state.api_url);
    let proj_url = &players_url[..players_url.rfind('/').expect("impossible")];

    let urls = [ proj_url, &players_url ];

    // fetch the project and players data
    let mut r = future::join_all(urls.iter().map(|url|
        get_data(&state.client, url)
    )).await
    .into_iter();

    let proj = r.next().expect("impossible").and_then(parse_data);
    let players = r.next().expect("impossible").and_then(parse_data);

    HtmlTemplate(ProjectTemplate(proj, players))
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

#[derive(Debug, Deserialize)]
pub struct Config {
    pub base_path: String,
    pub listen_ip: String,
    pub listen_port: u16,
    pub log_headers: bool,
    pub api_url: String
}

async fn run() -> Result<(), StartupError> {
    info!("Reading config.toml");
    let config: Config = toml::from_str(&fs::read_to_string("config.toml")?)?;

    let state = Arc::new(AppState {
        client: Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
        api_url: config.api_url
    });

    // set up router
    let app = routes(
        &config.base_path,
        config.log_headers
    )
    .with_state(state);

    // serve pages
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
    let file_appender = tracing_appender::rolling::daily("", "front.log");
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
