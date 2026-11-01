//! Env settings plus the route table from deploy/meter.toml.

use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

pub struct Config {
    pub listen: SocketAddr,
    pub upstream: String,
    pub redis_url: String,
    pub redis_conns: usize,
    pub redis_timeout: Duration,
    pub upstream_timeout: Duration,
    pub max_body: usize,
    pub instance: String,
    pub routes: Routes,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_num(name: &str, default: u64) -> u64 {
    env_or(name, &default.to_string())
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be a number"))
}

impl Config {
    pub fn from_env() -> Self {
        let meter_toml = env_or("METER_CONFIG", "deploy/meter.toml");
        let text = std::fs::read_to_string(&meter_toml)
            .unwrap_or_else(|e| panic!("reading {meter_toml}: {e}"));
        Config {
            listen: env_or("LISTEN", "0.0.0.0:80").parse().expect("LISTEN host:port"),
            upstream: env_or("UPSTREAM", "api:8000"),
            redis_url: env_or("REDIS_URL", "redis://127.0.0.1:6380/0"),
            redis_conns: env_num("REDIS_CONNS", 4) as usize,
            // REDIS_TIMEOUT in store.py
            redis_timeout: Duration::from_secs(env_num("REDIS_TIMEOUT", 5)),
            // nginx's proxy_read_timeout default
            upstream_timeout: Duration::from_secs(env_num("UPSTREAM_TIMEOUT", 60)),
            max_body: env_num("MAX_BODY", 1 << 20) as usize,
            // Docker sets HOSTNAME to the container id, like socket.gethostname() in store.py
            instance: format!("gateway-{}", env_or("HOSTNAME", "local")),
            routes: Routes::parse(&text),
        }
    }
}

pub enum Route {
    Fixed { feature: String, cost: i64 },
    Batch,
}

pub enum Cost {
    One(String, i64),
    /// Not a valid batch: Python answers it with its 422, nothing charged.
    Invalid,
    /// More than one feature in a batch: 501, as in meter.py.
    Mixed,
}

pub struct Routes {
    by_path: HashMap<String, Route>,
    /// A batch operation's `feature` must be one of these (the Operation union in schemas.py).
    features: HashSet<String>,
}

#[derive(Deserialize)]
struct MeterToml {
    routes: BTreeMap<String, RouteToml>,
}

#[derive(Deserialize)]
struct RouteToml {
    feature: Option<String>,
    cost: Option<i64>,
    kind: Option<String>,
}

impl Routes {
    pub fn parse(text: &str) -> Self {
        let parsed: MeterToml = toml::from_str(text).expect("meter.toml");
        let mut by_path = HashMap::new();
        let mut features = HashSet::new();
        for (path, r) in parsed.routes {
            let route = match (r.kind.as_deref(), r.feature) {
                (Some("batch"), _) => Route::Batch,
                (None, Some(feature)) => {
                    features.insert(feature.clone());
                    Route::Fixed { feature, cost: r.cost.unwrap_or(1) }
                }
                _ => panic!("meter.toml: {path} needs `feature` or kind = \"batch\""),
            };
            by_path.insert(path, route);
        }
        Routes { by_path, features }
    }

    pub fn get(&self, path: &str) -> Option<&Route> {
        self.by_path.get(path)
    }

    pub fn cost(&self, route: &Route, body: &[u8]) -> Cost {
        match route {
            Route::Fixed { feature, cost } => Cost::One(feature.clone(), *cost),
            Route::Batch => self.batch_cost(body),
        }
    }

    /// Counter(op.feature for op in BatchRequest.operations), after the checks BatchRequest
    /// makes on the fields this reads. Ops' other fields are left to Python: a bad one is a 422
    /// there, and the meter refunds it.
    fn batch_cost(&self, body: &[u8]) -> Cost {
        let Ok(v) = serde_json::from_slice::<Value>(body) else {
            return Cost::Invalid;
        };
        if v.get("mode").and_then(Value::as_str) != Some("all_or_nothing") {
            return Cost::Invalid;
        }
        let Some(ops) = v.get("operations").and_then(Value::as_array) else {
            return Cost::Invalid;
        };
        let mut counts: BTreeMap<&str, i64> = BTreeMap::new();
        for op in ops {
            match op.get("feature").and_then(Value::as_str) {
                Some(f) if self.features.contains(f) => *counts.entry(f).or_default() += 1,
                _ => return Cost::Invalid,
            }
        }
        match counts.len() {
            0 => Cost::Invalid, // min_length=1
            1 => {
                let (f, n) = counts.into_iter().next().unwrap();
                Cost::One(f.to_string(), n)
            }
            _ => Cost::Mixed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn routes() -> Routes {
        Routes::parse(include_str!("../../deploy/meter.toml"))
    }

    fn batch(body: &str) -> Cost {
        let r = routes();
        r.cost(r.get("/batch").unwrap(), body.as_bytes())
    }

    #[test]
    fn fixed_routes() {
        let r = routes();
        match r.cost(r.get("/container_tracking").unwrap(), b"") {
            Cost::One(f, c) => assert_eq!((f.as_str(), c), ("container-tracking", 1)),
            _ => panic!(),
        }
        assert!(r.get("/").is_none());
        assert!(r.get("/usage").is_none());
    }

    #[test]
    fn batch_counts_operations() {
        let body = r#"{"mode":"all_or_nothing","operations":[
            {"feature":"shipment-data","shipment_id":"a"},
            {"feature":"shipment-data","shipment_id":"b"}]}"#;
        match batch(body) {
            Cost::One(f, c) => assert_eq!((f.as_str(), c), ("shipment-data", 2)),
            _ => panic!(),
        }
    }

    #[test]
    fn batch_mixed() {
        let body = r#"{"mode":"all_or_nothing","operations":[
            {"feature":"shipment-data"},{"feature":"container-tracking"}]}"#;
        assert!(matches!(batch(body), Cost::Mixed));
    }

    #[test]
    fn batch_invalid() {
        for body in [
            "not json",
            r#"{"operations":[{"feature":"shipment-data"}]}"#,
            r#"{"mode":"best_effort","operations":[{"feature":"shipment-data"}]}"#,
            r#"{"mode":"all_or_nothing","operations":[]}"#,
            r#"{"mode":"all_or_nothing","operations":[{"feature":"nope"}]}"#,
            r#"{"mode":"all_or_nothing","operations":[{"container_id":"x"}]}"#,
        ] {
            assert!(matches!(batch(body), Cost::Invalid), "{body}");
        }
    }
}
