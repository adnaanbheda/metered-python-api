//! Metering gateway: identity, idempotency and quota in front of the Python replicas, which
//! serve the business routes unmetered (METER_MODE=gateway). Replaces nginx as the front door.

mod config;
mod errors;
mod fingerprint;
mod meter;
mod period;
mod store;
mod upstream;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::TcpListener;

/// `gateway --health`: exit 0 if something accepts on LISTEN. The runtime image has no curl.
fn health(listen: std::net::SocketAddr) -> ! {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], listen.port()));
    let ok = std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(2)).is_ok();
    std::process::exit(if ok { 0 } else { 1 })
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL")
                .unwrap_or_else(|_| "warn,gateway=info".into()),
        )
        .init();
    let cfg = config::Config::from_env();
    if std::env::args().any(|a| a == "--health") {
        health(cfg.listen);
    }

    let store = store::Store::connect(&cfg.redis_url, cfg.redis_conns, cfg.redis_timeout, cfg.instance).await;
    let upstream = upstream::Upstream::start(cfg.upstream, cfg.upstream_timeout).await;
    let state = Arc::new(meter::State {
        routes: cfg.routes,
        store,
        upstream,
        max_body: cfg.max_body,
    });

    let listener = TcpListener::bind(cfg.listen).await.expect("bind LISTEN");
    tracing::info!("listening on {}", cfg.listen);
    let stop = shutdown();
    tokio::pin!(stop);
    loop {
        let (sock, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("accept: {e}");
                    continue;
                }
            },
            _ = &mut stop => break,
        };
        let _ = sock.set_nodelay(true);
        let state = state.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| meter::handle(state.clone(), req));
            if let Err(e) = http1::Builder::new()
                .keep_alive(true)
                .serve_connection(TokioIo::new(sock), svc)
                .await
            {
                tracing::debug!("connection: {e}");
            }
        });
    }
}
