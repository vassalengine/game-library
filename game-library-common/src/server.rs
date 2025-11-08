use axum::{
    body::Body,
    extract::{ConnectInfo, Request}
};
use std::net::SocketAddr;
use tower_http::trace::MakeSpan;
use tracing::{info, info_span, Span};

pub fn real_addr(request: &Request) -> String {
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
