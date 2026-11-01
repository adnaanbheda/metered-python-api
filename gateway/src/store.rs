//! reserve / commit / refund against Redis: the same Lua files, keys and args as src/store.py.

use crate::period::current_period;
use redis::aio::ConnectionManager;
use redis::{RedisError, Script};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// RESERVATION_TTL, USAGE_STREAM, USAGE_STREAM_MAXLEN in store.py
const RESERVATION_TTL: i64 = 300;
const USAGE_STREAM: &str = "usage_events";
const USAGE_STREAM_MAXLEN: i64 = 1_000_000;

pub struct Reservation {
    pub org_id: String,
    pub feature: String,
    pub request_id: String,
    pub period: String,
    pub usage_key: String,
    pub reservation_key: String,
}

pub enum Reserved {
    Ok(Reservation),
    /// reserve.lua's status when it isn't "ok", with its used/limit values.
    Rejected { status: String, used: i64, limit: i64 },
}

#[derive(Debug)]
pub enum StoreError {
    Redis(#[allow(dead_code)] RedisError), // read through Debug, in the logs
    Timeout,
}

pub struct Store {
    /// Multiplexed: concurrent commands share each socket and go out pipelined.
    conns: Vec<ConnectionManager>,
    next: AtomicUsize,
    timeout: Duration,
    instance: String,
    reserve: Script,
    commit: Script,
    refund: Script,
}

// {org_id} hash tag: all of an org's keys land in one Cluster slot
fn limit_key(org: &str, feature: &str, period: &str) -> String {
    format!("quota:{{{org}}}:{feature}:limit:{period}")
}

fn usage_key(org: &str, feature: &str, period: &str) -> String {
    format!("quota:{{{org}}}:{feature}:usage:{period}")
}

fn reservation_key(org: &str, request_id: &str) -> String {
    format!("res:{{{org}}}:{request_id}")
}

impl Store {
    /// Retries until Redis answers, like the replicas waiting on compose's healthcheck.
    pub async fn connect(url: &str, n: usize, timeout: Duration, instance: String) -> Self {
        let client = redis::Client::open(url).expect("REDIS_URL");
        let mut conns = Vec::with_capacity(n);
        while conns.len() < n {
            match ConnectionManager::new(client.clone()).await {
                Ok(c) => conns.push(c),
                Err(e) => {
                    tracing::warn!("redis not ready: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Store {
            conns,
            next: AtomicUsize::new(0),
            timeout,
            instance,
            reserve: Script::new(include_str!("../../src/lua/reserve.lua")),
            commit: Script::new(include_str!("../../src/lua/commit.lua")),
            refund: Script::new(include_str!("../../src/lua/refund.lua")),
        }
    }

    fn conn(&self) -> ConnectionManager {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        self.conns[i].clone()
    }

    /// One deadline per store call, as store.py does with asyncio.timeout.
    async fn run<T: redis::FromRedisValue>(
        &self,
        call: redis::ScriptInvocation<'_>,
    ) -> Result<T, StoreError> {
        let mut conn = self.conn();
        // invoke_async: EVALSHA, and SCRIPT LOAD + retry on NOSCRIPT (e.g. after a Redis restart)
        match tokio::time::timeout(self.timeout, call.invoke_async(&mut conn)).await {
            Ok(r) => r.map_err(StoreError::Redis),
            Err(_) => Err(StoreError::Timeout),
        }
    }

    pub async fn reserve(
        &self,
        org_id: &str,
        feature: &str,
        request_id: &str,
        cost: i64,
        fingerprint: &str,
    ) -> Result<Reserved, StoreError> {
        let (period, expire_at) = current_period();
        let usage_key = usage_key(org_id, feature, &period);
        let reservation_key = reservation_key(org_id, request_id);
        let mut call = self.reserve.prepare_invoke();
        call.key(limit_key(org_id, feature, &period))
            .key(&usage_key)
            .key(&reservation_key)
            .arg(cost)
            .arg(fingerprint)
            .arg(expire_at)
            .arg(RESERVATION_TTL);
        let (status, used, limit): (String, i64, i64) = self.run(call).await?;
        if status != "ok" {
            return Ok(Reserved::Rejected { status, used, limit });
        }
        Ok(Reserved::Ok(Reservation {
            org_id: org_id.to_string(),
            feature: feature.to_string(),
            request_id: request_id.to_string(),
            period,
            usage_key,
            reservation_key,
        }))
    }

    /// false: the reservation was no longer 'reserved' (expired mid-request), as in store.py.
    pub async fn commit(&self, r: &Reservation) -> Result<bool, StoreError> {
        let mut call = self.commit.prepare_invoke();
        call.key(&r.reservation_key)
            .key(USAGE_STREAM)
            .arg(&r.org_id)
            .arg(&r.feature)
            .arg(&r.request_id)
            .arg(&r.period)
            .arg(USAGE_STREAM_MAXLEN)
            .arg(&self.instance);
        let ok: i64 = self.run(call).await?;
        Ok(ok == 1)
    }

    pub async fn refund(&self, r: &Reservation) -> Result<bool, StoreError> {
        let mut call = self.refund.prepare_invoke();
        call.key(&r.reservation_key).key(&r.usage_key);
        let ok: i64 = self.run(call).await?;
        Ok(ok == 1)
    }
}
