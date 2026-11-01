//! JSON error responses with the same status, code and detail as the Python meter
//! (src/exceptions.py, src/utils/responses.py).

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use serde::Serialize;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type Body = BoxBody<Bytes, BoxError>;

pub const REQUEST_ID: &str = "x-request-id";

pub fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed()
}

// Field order matters: Starlette renders {"detail":...,"code":...}.
#[derive(Serialize)]
struct ErrorBody<'a> {
    detail: &'a str,
    code: Option<&'a str>,
}

pub fn json_error(
    status: u16,
    code: Option<&str>,
    detail: &str,
    request_id: Option<&str>,
) -> Response<Body> {
    let body = serde_json::to_vec(&ErrorBody { detail, code }).expect("plain struct");
    let mut resp = Response::new(full(body));
    *resp.status_mut() = StatusCode::from_u16(status).expect("valid status");
    let h = resp.headers_mut();
    h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(id) = request_id.and_then(|id| HeaderValue::from_str(id).ok()) {
        h.insert(REQUEST_ID, id);
    }
    resp
}

/// reserve.lua's non-ok status -> response, as exceptions.BY_STATUS maps it.
pub fn reserve_rejected(
    status: &str,
    feature: &str,
    used: i64,
    limit: i64,
    request_id: &str,
) -> Response<Body> {
    let id = Some(request_id);
    match status {
        "reserved" => json_error(
            409,
            Some("request_in_progress"),
            "request with this id is still processing",
            id,
        ),
        "committed" => json_error(
            409,
            Some("already_processed"),
            "request with this id was already processed",
            id,
        ),
        "mismatch" => json_error(
            422,
            Some("request_id_reused"),
            "request id reused with a different request",
            id,
        ),
        "unprovisioned" => json_error(
            403,
            Some("quota_not_provisioned"),
            "no quota provisioned for this period",
            id,
        ),
        "exceeded" => json_error(
            429,
            Some("quota_exceeded"),
            &format!("quota exceeded for {feature}: {used}/{limit}"),
            id,
        ),
        other => json_error(
            500,
            Some("quota_error"),
            &format!("unexpected reserve status '{other}'"),
            id,
        ),
    }
}
