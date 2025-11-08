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
use glc::{
    model::{ProjectData, Users},
    server::{real_addr, setup_logging, shutdown_signal, SpanMaker}
};
use http::header::{CACHE_CONTROL, HeaderValue};
use mime::APPLICATION_JSON;
use reqwest::{
    Client,
    header::ACCEPT
};
use serde::{
    Deserialize,
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
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer}
};
use tracing::{error, info, Level};
use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::SubscriberInitExt
};

const SITE_DIR: &str = "../../../site";
const DIST_DIR: &str = "app/dist";

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
    let _guard = setup_logging("front.log");

    info!("Starting");

    if let Err(e) = run().await {
        error!("{}", e);
    }

    info!("Exiting");
}
