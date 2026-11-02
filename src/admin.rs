//! Admin read-modify-write: merge, diff, guards, set-message construction. Pure functions.
//!
//! Firmware replaces a whole config section on set (2.7.x: `config.security = incoming`), so every write
//! sends the complete current object with changes merged in. A security set without a valid private key
//! makes 2.7.x generate a new keypair, so secret values come from the fresh unredacted read, never from
//! the caller.

use anyhow::{Result, bail};
use base64::Engine;
use serde_json::{Map, Value, json};

pub const REDACTED: &str = "<redacted>";

/// What a get response refers to and how to write it back.
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    /// AdminMessage set field: set_config, set_module_config, set_channel, set_owner.
    pub set_field: &'static str,
    /// Variant inside Config/ModuleConfig (e.g. "lora", "mqtt"); None for channel/owner.
    pub variant: Option<String>,
    /// Current object (unredacted).
    pub current: Value,
}

/// Parse an unredacted AdminMessage get response.
pub fn target_from_response(resp: &Value) -> Result<Target> {
    let o = resp.as_object().ok_or_else(|| anyhow::anyhow!("response is not an object"))?;
    for (get, set) in [("get_config_response", "set_config"), ("get_module_config_response", "set_module_config")] {
        if let Some(v) = o.get(get) {
            let inner = v.as_object().filter(|m| m.len() == 1).ok_or_else(|| {
                anyhow::anyhow!("{get} has no single variant (unsupported by this firmware?): {v}")
            })?;
            let (k, cur) = inner.iter().next().unwrap();
            return Ok(Target { set_field: set, variant: Some(k.clone()), current: cur.clone() });
        }
    }
    if let Some(v) = o.get("get_channel_response") {
        return Ok(Target { set_field: "set_channel", variant: None, current: v.clone() });
    }
    if let Some(v) = o.get("get_owner_response") {
        return Ok(Target { set_field: "set_owner", variant: None, current: v.clone() });
    }
    bail!("not a get_*_response: {}", Value::Object(o.clone()))
}

pub fn contains_redacted(v: &Value) -> bool {
    match v {
        Value::String(s) => s == REDACTED,
        Value::Array(a) => a.iter().any(contains_redacted),
        Value::Object(m) => m.values().any(contains_redacted),
        _ => false,
    }
}

/// Deep merge: objects merge recursively, everything else (incl. arrays) replaces, null removes the key.
pub fn merge(base: &Value, changes: &Value) -> Value {
    match (base, changes) {
        (Value::Object(b), Value::Object(c)) => {
            let mut out = b.clone();
            for (k, v) in c {
                if v.is_null() {
                    out.remove(k);
                } else {
                    let nv = match out.get(k) {
                        Some(old) => merge(old, v),
                        None => merge(&Value::Null, v),
                    };
                    out.insert(k.clone(), nv);
                }
            }
            Value::Object(out)
        }
        (_, Value::Object(c)) => merge(&Value::Object(Map::new()), &Value::Object(c.clone())),
        (_, c) => c.clone(),
    }
}

/// proto3 default: absent and default values are equivalent on the wire.
pub fn is_default(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => true,
        Some(Value::Bool(b)) => !b,
        Some(Value::Number(n)) => n.as_f64() == Some(0.0),
        Some(Value::String(s)) => s.is_empty(),
        Some(Value::Array(a)) => a.is_empty(),
        Some(Value::Object(m)) => m.is_empty(),
    }
}

fn eq(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (Some(Value::Object(x)), Some(Value::Object(y))) => {
            x.keys().chain(y.keys()).all(|k| eq(x.get(k), y.get(k)))
        }
        _ => a == b || (is_default(a) && is_default(b)),
    }
}

/// Leaf-level differences: (path, before, after).
pub fn diff(before: &Value, after: &Value) -> Vec<(String, Value, Value)> {
    let mut out = Vec::new();
    diff_rec("", Some(before), Some(after), &mut out);
    out
}

fn diff_rec(path: &str, a: Option<&Value>, b: Option<&Value>, out: &mut Vec<(String, Value, Value)>) {
    match (a, b) {
        (Some(Value::Object(x)), Some(Value::Object(y))) => {
            let mut keys: Vec<&String> = x.keys().chain(y.keys()).collect();
            keys.sort();
            keys.dedup();
            for k in keys {
                let p = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                diff_rec(&p, x.get(k), y.get(k), out);
            }
        }
        _ if eq(a, b) => {}
        _ => out.push((path.to_string(), a.cloned().unwrap_or(Value::Null), b.cloned().unwrap_or(Value::Null))),
    }
}

/// Is this write able to cut us off from the node or change its identity/radio?
pub fn is_disruptive(t: &Target) -> bool {
    match (t.set_field, t.variant.as_deref()) {
        ("set_config", Some("security" | "lora")) => true,
        ("set_channel", _) => t.current.get("index").and_then(|i| i.as_u64()).unwrap_or(0) == 0,
        _ => false,
    }
}

/// Security-specific invariants on the merged object.
pub fn check_security(merged: &Value, our_public_b64: &str) -> Result<()> {
    let pk = merged.get("private_key").and_then(|v| v.as_str()).unwrap_or("");
    let ok = base64::engine::general_purpose::STANDARD.decode(pk).map(|b| b.len() == 32).unwrap_or(false);
    if !ok {
        bail!("security write refused: merged private_key is not 32 bytes (firmware 2.7.x would generate a new keypair)");
    }
    let admins: Vec<&str> = merged
        .get("admin_key")
        .and_then(|a| a.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
        .unwrap_or_default();
    if !admins.contains(&our_public_b64) {
        bail!("security write refused: our public key would be removed from admin_key (lockout)");
    }
    Ok(())
}

/// AdminMessage JSON for the write.
pub fn set_message(t: &Target, merged: &Value, passkey: &str) -> Value {
    let body = match &t.variant {
        Some(v) => json!({ v.clone(): merged }),
        None => merged.clone(),
    };
    json!({ t.set_field: body, "session_passkey": passkey })
}

/// Paths in `changes` (leaf level) that do not match `readback`.
pub fn verify(expected: &Value, readback: &Value, changes: &Value) -> Vec<(String, Value, Value)> {
    let mut paths = Vec::new();
    leaf_paths("", changes, &mut paths);
    let mut bad = Vec::new();
    for p in paths {
        let e = get_path(expected, &p);
        let r = get_path(readback, &p);
        if !eq(e, r) {
            bad.push((p, e.cloned().unwrap_or(Value::Null), r.cloned().unwrap_or(Value::Null)));
        }
    }
    bad
}

fn leaf_paths(prefix: &str, v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) if !m.is_empty() => {
            for (k, x) in m {
                let p = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                leaf_paths(&p, x, out);
            }
        }
        _ => out.push(prefix.to_string()),
    }
}

fn get_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(v, |cur, k| cur.get(k))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_and_diff() {
        let base = json!({"a": 1, "b": {"c": true, "d": "x"}, "arr": [1, 2]});
        let m = merge(&base, &json!({"b": {"c": false}, "arr": [3], "e": 5, "a": null}));
        assert_eq!(m, json!({"b": {"c": false, "d": "x"}, "arr": [3], "e": 5}));
        let d = diff(&base, &m);
        let paths: Vec<&str> = d.iter().map(|x| x.0.as_str()).collect();
        assert_eq!(paths, vec!["a", "arr", "b.c", "e"]);
    }

    #[test]
    fn default_equivalence() {
        assert!(diff(&json!({"x": 0, "y": false}), &json!({})).is_empty());
        assert!(verify(&json!({"x": false}), &json!({}), &json!({"x": false})).is_empty());
        assert_eq!(verify(&json!({"x": 5}), &json!({"x": 4}), &json!({"x": 5})).len(), 1);
    }

    #[test]
    fn targets_and_guards() {
        let r = json!({"get_config_response": {"security": {"private_key": "K", "admin_key": ["A"]}}, "session_passkey": "p"});
        let t = target_from_response(&r).unwrap();
        assert_eq!((t.set_field, t.variant.as_deref()), ("set_config", Some("security")));
        assert!(is_disruptive(&t));
        let key = base64::engine::general_purpose::STANDARD.encode([1u8; 32]);
        assert!(check_security(&json!({"private_key": key, "admin_key": ["OUR"]}), "OUR").is_ok());
        assert!(check_security(&json!({"private_key": key, "admin_key": ["X"]}), "OUR").is_err());
        assert!(check_security(&json!({"admin_key": ["OUR"]}), "OUR").is_err());
        assert!(contains_redacted(&json!({"a": ["<redacted>"]})));
        let ch = target_from_response(&json!({"get_channel_response": {"index": 1, "settings": {}}})).unwrap();
        assert!(!is_disruptive(&ch));
        assert!(is_disruptive(&target_from_response(&json!({"get_channel_response": {"settings": {}}})).unwrap()));
        assert!(target_from_response(&json!({"get_module_config_response": {}})).is_err());
        let m = set_message(&t, &json!({"x": 1}), "pk");
        assert_eq!(m, json!({"set_config": {"security": {"x": 1}}, "session_passkey": "pk"}));
    }
}
