//! Round-robin HTTP/1.1 proxy to the api replicas: what nginx's `upstream api` block did.

use crate::errors::{full, Body, BoxError};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Body as _;
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue, CONNECTION, TRANSFER_ENCODING};
use hyper::http::request::Parts;
use hyper::{HeaderMap, Request, Response, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub const X_UPSTREAM: HeaderName = HeaderName::from_static("x-upstream");
const REFRESH: Duration = Duration::from_secs(2);

pub enum UpstreamError {
    Unavailable(String),
    Timeout,
}

pub struct Upstream {
    target: String,
    addrs: RwLock<Arc<Vec<SocketAddr>>>,
    next: AtomicUsize,
    client: Client<HttpConnector, Body>,
    pub timeout: Duration,
}

impl Upstream {
    /// Waits until `target` (e.g. "api:8000") resolves, then re-resolves it every 2s. Docker
    /// DNS returns one A record per replica, so a scaled or recreated replica is picked up;
    /// nginx resolves once at startup and needed a restart for that.
    pub async fn start(target: String, timeout: Duration) -> Arc<Self> {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let up = Arc::new(Upstream {
            target,
            addrs: RwLock::new(Arc::new(Vec::new())),
            next: AtomicUsize::new(0),
            client: Client::builder(TokioExecutor::new())
                // under uvicorn's 5s keep-alive, so the replica never closes a connection
                // the pool is about to reuse
                .pool_idle_timeout(Duration::from_secs(4))
                .build(connector),
            timeout,
        });
        while !up.resolve().await {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let bg = up.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(REFRESH).await;
                bg.resolve().await;
            }
        });
        up
    }

    /// Keeps the last good set when a lookup fails or comes back empty.
    async fn resolve(&self) -> bool {
        match tokio::net::lookup_host(&self.target).await {
            Ok(found) => {
                let mut addrs: Vec<SocketAddr> = found.filter(|a| a.is_ipv4()).collect();
                if addrs.is_empty() {
                    return false;
                }
                addrs.sort();
                addrs.dedup();
                let mut cur = self.addrs.write().unwrap();
                if **cur != addrs {
                    tracing::info!("upstream {} -> {:?}", self.target, addrs);
                    *cur = Arc::new(addrs);
                }
                true
            }
            Err(e) => {
                tracing::warn!("resolving {}: {e}", self.target);
                false
            }
        }
    }

    fn pick(&self) -> Option<SocketAddr> {
        let addrs = self.addrs.read().unwrap().clone();
        if addrs.is_empty() {
            return None;
        }
        let i = self.next.fetch_add(1, Ordering::Relaxed) % addrs.len();
        Some(addrs[i])
    }

    /// Sends the request to the next replica. Only the response head is awaited here.
    /// A replica that fails before answering gets the request again on the next replica, once,
    /// as nginx's proxy_next_upstream does: if the body can be resent (buffered, or none) and
    /// the request is idempotent or never left the gateway (connect error).
    pub async fn send(
        &self,
        mut parts: Parts,
        payload: Payload,
    ) -> Result<(SocketAddr, Response<Incoming>), UpstreamError> {
        strip_hop_by_hop(&mut parts.headers);
        let resend = match &payload {
            Payload::Buffered(b) => Some(b.clone()),
            Payload::Stream(b) if b.is_end_stream() => Some(Bytes::new()),
            Payload::Stream(_) => None,
        };
        let body = match payload {
            Payload::Buffered(b) => full(b),
            Payload::Stream(b) => b,
        };
        match self.attempt(&parts, body).await {
            Err(Attempt::Failed(e)) => {
                let retry = e.is_connect() || parts.method.is_idempotent();
                match resend.filter(|_| retry) {
                    Some(b) => {
                        tracing::info!("retrying on the next replica: {}", chain(&e));
                        self.attempt(&parts, full(b)).await.map_err(Attempt::into_error)
                    }
                    None => Err(Attempt::Failed(e).into_error()),
                }
            }
            other => other.map_err(Attempt::into_error),
        }
    }

    async fn attempt(
        &self,
        parts: &Parts,
        body: Body,
    ) -> Result<(SocketAddr, Response<Incoming>), Attempt> {
        let addr = self.pick().ok_or(Attempt::NoAddress)?;
        let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        let uri = Uri::try_from(format!("http://{addr}{path}")).map_err(|_| Attempt::NoAddress)?;
        let mut req = Request::builder()
            .method(parts.method.clone())
            .uri(uri)
            .version(hyper::Version::HTTP_11)
            .body(body)
            .expect("parts came from a valid request");
        *req.headers_mut() = parts.headers.clone();
        match tokio::time::timeout(self.timeout, self.client.request(req)).await {
            Ok(Ok(resp)) => Ok((addr, resp)),
            Ok(Err(e)) => Err(Attempt::Failed(e)),
            Err(_) => Err(Attempt::Timeout),
        }
    }
}

/// The request body as handed to `send`: buffered bodies can be resent on another replica.
pub enum Payload {
    Buffered(Bytes),
    Stream(Body),
}

enum Attempt {
    NoAddress,
    Failed(hyper_util::client::legacy::Error),
    Timeout,
}

impl Attempt {
    fn into_error(self) -> UpstreamError {
        match self {
            Attempt::NoAddress => UpstreamError::Unavailable("no upstream address".into()),
            Attempt::Failed(e) => UpstreamError::Unavailable(chain(&e)),
            Attempt::Timeout => UpstreamError::Timeout,
        }
    }
}

/// "client error (SendRequest): connection closed before message completed", not just the head.
fn chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        s.push_str(": ");
        s.push_str(&cause.to_string());
        source = cause.source();
    }
    s
}

/// Connection-level headers belong to one hop. Content-Length goes too: hyper sets it again
/// from the body it is given, buffered or streamed.
pub fn strip_hop_by_hop(h: &mut HeaderMap) {
    for name in [CONNECTION, TRANSFER_ENCODING, hyper::header::CONTENT_LENGTH] {
        h.remove(name);
    }
    for name in ["keep-alive", "proxy-connection", "te", "trailer", "upgrade"] {
        h.remove(name);
    }
}

pub fn upstream_header(addr: SocketAddr) -> HeaderValue {
    HeaderValue::from_str(&addr.to_string()).expect("socket addr is ascii")
}

pub fn stream(body: Incoming) -> Body {
    body.map_err(|e| Box::new(e) as BoxError).boxed()
}
