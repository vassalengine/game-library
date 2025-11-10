use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request}
};
use std::net::{IpAddr, SocketAddr};
use tokio::net::TcpListener;
use tower_http::trace::MakeSpan;
use tracing::{info, info_span, Span};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_panic::panic_hook;
use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::SubscriberInitExt
};

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
pub struct SpanMaker {
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

impl Default for SpanMaker {
    fn default() -> Self {
        Self::new()
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

pub fn setup_logging(crate_name: &str, log_base: &str) -> WorkerGuard {
    // set up logging
    // TODO: make log location configurable
    let file_appender = tracing_appender::rolling::daily("", log_base);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| {
                [
                    // log calling crate at info level
                    &format!("{}=info", crate_name),
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

    guard
}

pub async fn shutdown_signal() {
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

pub async fn serve(
    app: Router,
    ip: IpAddr,
    port: u16
) -> Result<(), std::io::Error>
{
    let addr = SocketAddr::from((ip, port));
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>()
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use nix::{
        sys::{self, signal::Signal},
        unistd::Pid
    };
    use std::net::Ipv4Addr;

    #[track_caller]
    async fn assert_shutdown(sig: Signal) {
        let app = Router::new();
        let pid = Pid::this();

        let server_handle = tokio::spawn(
            serve(app, IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
                .into_future()
        );

        // ensure that the server has a chance to start
        tokio::task::yield_now().await;

        sys::signal::kill(pid, sig).unwrap();

        server_handle.await.unwrap().unwrap();
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
}
