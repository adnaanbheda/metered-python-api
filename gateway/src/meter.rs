//! The request path: MeterMiddleware's steps, in its order, so status codes and bodies match.
//!
//! unmetered path -> stream to a replica
//! metered: org id -> request id -> body -> cost -> reserve -> proxy -> respond -> settle

use crate::config::{Cost, Routes};
use crate::errors::{full, json_error, reserve_rejected, Body, REQUEST_ID};
use crate::fingerprint::{fingerprint, unquote};
use crate::store::{Reservation, Reserved, Store};
use crate::upstream::{
    stream, strip_hop_by_hop, upstream_header, Payload, Upstream, UpstreamError, X_UPSTREAM,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue};
use hyper::http::request::Parts;
use hyper::{HeaderMap, Request, Response};
use std::convert::Infallible;
use std::sync::Arc;

const ORG_HEADER: &str = "x-org-id";
const MAX_REQUEST_ID_LEN: usize = 128;
// What the gateway tells the replica; GatewayGuard (src/middlewares/gateway_guard.py) checks
// one of them is present on every metered path. Clients can't send them: stripped below.
const X_METER_RESERVATION: HeaderName = HeaderName::from_static("x-meter-reservation");
const X_METER_SKIP: HeaderName = HeaderName::from_static("x-meter-skip");

pub struct State {
    pub routes: Routes,
    pub store: Store,
    pub upstream: Arc<Upstream>,
    pub max_body: usize,
}

/// Starlette decodes header bytes as latin-1; same here, so a non-UTF-8 byte can't differ.
fn header(h: &HeaderMap, name: &str) -> String {
    h.get(name)
        .map(|v| v.as_bytes().iter().map(|&b| b as char).collect::<String>())
        .unwrap_or_default()
        .trim()
        .to_string()
}

pub async fn handle(state: Arc<State>, req: Request<Incoming>) -> Result<Response<Body>, Infallible> {
    let (mut parts, body) = req.into_parts();
    for name in [X_METER_RESERVATION, X_METER_SKIP] {
        parts.headers.remove(name);
    }
    // ASGI's scope["path"]: percent-decoded, which is what meter.toml and the fingerprint use
    let path = unquote(parts.uri.path());

    let Some(route) = state.routes.get(&path) else {
        return Ok(proxy(&state, parts, Payload::Stream(stream(body))).await);
    };
    // No org: IdentityMiddleware answers the 401 before anything is charged.
    let org_id = header(&parts.headers, ORG_HEADER);
    if org_id.is_empty() {
        return Ok(proxy(&state, parts, Payload::Stream(stream(body))).await);
    }

    let mut request_id = header(&parts.headers, REQUEST_ID);
    if request_id.is_empty() {
        request_id = uuid::Uuid::new_v4().simple().to_string();
    }
    if request_id.chars().count() > MAX_REQUEST_ID_LEN {
        let detail = format!("X-Request-Id longer than {MAX_REQUEST_ID_LEN}");
        return Ok(json_error(400, Some("invalid_request_id"), &detail, None));
    }
    let rid = Some(request_id.as_str());

    let body = match Limited::new(body, state.max_body).collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => {
            let detail = format!("request body over {} bytes", state.max_body);
            return Ok(json_error(413, Some("body_too_large"), &detail, rid));
        }
        // client went away mid-body: nothing charged, nobody to answer
        Err(_) => return Ok(json_error(400, Some("body_read_failed"), "client disconnected", rid)),
    };

    let (feature, cost) = match state.routes.cost(route, &body) {
        Cost::One(f, c) => (f, c),
        Cost::Invalid => {
            parts.headers.insert(X_METER_SKIP, HeaderValue::from_static("invalid-body"));
            return Ok(proxy(&state, parts, Payload::Buffered(body)).await);
        }
        Cost::Mixed => {
            return Ok(json_error(
                501,
                Some("mixed_batch_unsupported"),
                "batches spanning multiple features not metered yet",
                rid,
            ))
        }
    };

    let query = parts.uri.query().unwrap_or("");
    let fp = fingerprint(parts.method.as_str(), &path, query, &body);
    let reservation = match state.store.reserve(&org_id, &feature, &request_id, cost, &fp).await {
        Ok(Reserved::Ok(r)) => r,
        Ok(Reserved::Rejected { status, used, limit }) => {
            tracing::debug!("rejected {request_id} {org_id}/{feature}: {status}");
            return Ok(reserve_rejected(&status, &feature, used, limit, &request_id));
        }
        // Fail closed: never serve a metered route without a reservation.
        Err(e) => {
            tracing::error!("reserve failed {request_id}: {e:?}");
            return Ok(json_error(500, Some("meter_unavailable"), "quota store unavailable", rid));
        }
    };

    let id = HeaderValue::from_str(&request_id).expect("came from a header or a uuid");
    parts.headers.insert(REQUEST_ID, id.clone());
    parts.headers.insert(X_METER_RESERVATION, id);
    // Spawned: hyper drops this future if the client disconnects, which would skip the settle
    // and leave the reservation 'reserved' until its TTL. The task runs to the end regardless.
    let task = tokio::spawn(forward_and_settle(state, parts, body, reservation));
    Ok(task.await.unwrap_or_else(|e| {
        tracing::error!("proxy task failed: {e}");
        json_error(500, Some("internal_error"), "internal error", rid)
    }))
}

/// Unmetered (or not chargeable) request: stream both ways.
async fn proxy(state: &State, parts: Parts, payload: Payload) -> Response<Body> {
    match state.upstream.send(parts, payload).await {
        Ok((addr, resp)) => {
            let (mut head, body) = resp.into_parts();
            strip_hop_by_hop(&mut head.headers);
            head.headers.insert(X_UPSTREAM, upstream_header(addr));
            Response::from_parts(head, stream(body))
        }
        Err(e) => upstream_error(e, None),
    }
}

/// Proxy a reserved request, answer the client, then commit (2xx) or refund (anything else).
/// The response body is read in full first: "the replica served it" means status and body.
async fn forward_and_settle(
    state: Arc<State>,
    parts: Parts,
    body: Bytes,
    res: Reservation,
) -> Response<Body> {
    let rid = res.request_id.clone();
    let deadline = state.upstream.timeout;
    let served = tokio::time::timeout(deadline, async {
        let (addr, resp) = state.upstream.send(parts, Payload::Buffered(body)).await?;
        let (head, body) = resp.into_parts();
        let bytes = body
            .collect()
            .await
            .map_err(|e| UpstreamError::Unavailable(e.to_string()))?
            .to_bytes();
        Ok::<_, UpstreamError>((addr, head, bytes))
    })
    .await
    .unwrap_or(Err(UpstreamError::Timeout));

    let (resp, ok) = match served {
        Ok((addr, mut head, bytes)) => {
            strip_hop_by_hop(&mut head.headers);
            head.headers.insert(X_UPSTREAM, upstream_header(addr));
            head.headers.insert(REQUEST_ID, HeaderValue::from_str(&rid).unwrap());
            let ok = head.status.is_success();
            (Response::from_parts(head, full(bytes)), ok)
        }
        Err(e) => (upstream_error(e, Some(&rid)), false),
    };
    // Off the response path, like the Python meter's commit after the body is sent.
    tokio::spawn(settle(state, res, ok));
    resp
}

/// The response is already decided, so a Redis failure here can't change it: log the key.
async fn settle(state: Arc<State>, res: Reservation, ok: bool) {
    let result = if ok {
        state.store.commit(&res).await
    } else {
        state.store.refund(&res).await
    };
    let action = if ok { "commit" } else { "refund" };
    match result {
        Ok(true) => tracing::debug!("{action} {}", res.request_id),
        Ok(false) => tracing::warn!("{action} skipped, reservation not live: {}", res.reservation_key),
        Err(e) => tracing::error!("{action} failed: {} {e:?}", res.reservation_key),
    }
}

fn upstream_error(e: UpstreamError, rid: Option<&str>) -> Response<Body> {
    match e {
        UpstreamError::Unavailable(why) => {
            tracing::warn!("upstream unavailable: {why}");
            json_error(502, Some("upstream_unavailable"), "upstream unavailable", rid)
        }
        UpstreamError::Timeout => json_error(504, Some("upstream_timeout"), "upstream timed out", rid),
    }
}
