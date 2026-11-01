//! Request fingerprint, byte-identical to src/utils/fingerprint.py so a retry hashes the same
//! whichever meter saw it first: sha256 of method|path|sorted query|canonical JSON body, first
//! 16 hex chars. Checked against Python's output in tests/vectors.json.

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt::Write;

/// `path` is the percent-decoded path (ASGI's scope["path"]), `query` the raw query string.
pub fn fingerprint(method: &str, path: &str, query: &str, body: &[u8]) -> String {
    let query = canonical_query(query);
    let body = canonical_body(body);
    let mut h = Sha256::new();
    for (i, part) in [method.as_bytes(), path.as_bytes(), query.as_bytes(), &body]
        .iter()
        .enumerate()
    {
        if i > 0 {
            h.update(b"|");
        }
        h.update(part);
    }
    hex::encode(h.finalize())[..16].to_string()
}

/// urlencode(sorted(parse_qsl(query, keep_blank_values=True)))
fn canonical_query(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (unquote_plus(k), unquote_plus(v))
        })
        .collect();
    pairs.sort();
    let mut out = String::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        quote_plus(&mut out, k);
        out.push('=');
        quote_plus(&mut out, v);
    }
    out
}

pub fn unquote(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(hi), Some(lo)) = (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

fn unquote_plus(s: &str) -> String {
    unquote(&s.replace('+', " "))
}

/// Python's quote_plus: letters, digits and `_.-~` stay, space becomes `+`, the rest `%XX`.
fn quote_plus(out: &mut String, s: &str) {
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => write!(out, "%{b:02X}").unwrap(),
        }
    }
}

/// json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")); raw bytes if not JSON.
fn canonical_body(body: &[u8]) -> Vec<u8> {
    if body.is_empty() {
        return Vec::new();
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(v) => {
            let mut out = String::with_capacity(body.len());
            write_value(&mut out, &v);
            out.into_bytes()
        }
        Err(_) => body.to_vec(),
    }
}

// serde_json's Map is a BTreeMap (no preserve_order feature), so keys come out sorted.
fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => write!(out, "{i}").unwrap(),
            (_, Some(u), _) => write!(out, "{u}").unwrap(),
            (_, _, Some(f)) => python_float(out, f),
            _ => unreachable!("serde_json number is i64, u64 or f64"),
        },
        Value::String(s) => write_str(out, s),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, x);
            }
            out.push(']');
        }
        Value::Object(m) => {
            out.push('{');
            for (i, (k, x)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_str(out, k);
                out.push(':');
                write_value(out, x);
            }
            out.push('}');
        }
    }
}

/// ensure_ascii=True: anything outside space..~ is escaped, non-BMP as a surrogate pair.
fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ' '..='~' => out.push(c),
            _ => {
                let mut units = [0u16; 2];
                for u in c.encode_utf16(&mut units) {
                    write!(out, "\\u{u:04x}").unwrap();
                }
            }
        }
    }
    out.push('"');
}

/// float.__repr__: shortest round-trip digits, positional for 1e-4 <= |f| < 1e16, else
/// scientific with a signed, at least 2-digit exponent (1e+16, 1e-05).
fn python_float(out: &mut String, f: f64) {
    if f == 0.0 {
        out.push_str(if f.is_sign_negative() { "-0.0" } else { "0.0" });
        return;
    }
    let sci = format!("{:e}", f); // shortest digits: "-1.2345e-5"
    let (mantissa, exp) = sci.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |m| ("-", m));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    out.push_str(sign);
    if (-4..16).contains(&exp) {
        let point = exp + 1; // digits before the decimal point
        if point <= 0 {
            out.push_str("0.");
            out.push_str(&"0".repeat(-point as usize));
            out.push_str(&digits);
        } else if point as usize >= digits.len() {
            out.push_str(&digits);
            out.push_str(&"0".repeat(point as usize - digits.len()));
            out.push_str(".0");
        } else {
            out.push_str(&digits[..point as usize]);
            out.push('.');
            out.push_str(&digits[point as usize..]);
        }
    } else {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        write!(out, "e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Vector {
        method: String,
        path: String,
        query: String,
        body: String,
        fingerprint: String,
    }

    /// Generated by scripts/gateway_vectors.py from the Python implementation.
    #[test]
    fn matches_python() {
        let raw = include_str!("../tests/vectors.json");
        let vectors: Vec<Vector> =
            serde_json::from_value(serde_json::from_str::<Value>(raw).unwrap()["fingerprints"].clone())
                .unwrap();
        assert!(!vectors.is_empty());
        for v in vectors {
            let got = fingerprint(&v.method, &v.path, &v.query, v.body.as_bytes());
            assert_eq!(got, v.fingerprint, "{} {}?{} {:?}", v.method, v.path, v.query, v.body);
        }
    }

    #[test]
    fn floats_print_like_python() {
        for (f, want) in [
            (1.5, "1.5"),
            (1.0, "1.0"),
            (1e16, "1e+16"),
            (1.5e16, "1.5e+16"),
            (123456789012345.6, "123456789012345.6"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (-2.5e-7, "-2.5e-07"),
            (-0.0, "-0.0"),
            (1e100, "1e+100"),
        ] {
            let mut out = String::new();
            python_float(&mut out, f);
            assert_eq!(out, want);
        }
    }
}
