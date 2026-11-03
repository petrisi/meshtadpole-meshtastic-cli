mod admin;
mod channel;
mod config;
mod daemon;
mod decode;
mod mesh;
mod packet;
mod pkc;
mod proto;
mod store;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use clap::{Parser, Subcommand};
use config::Config;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

/// Exit codes.
mod exit {
    pub const ERROR: i32 = 1;
    pub const NO_DEVICE: i32 = 3;
    pub const NO_DAEMON: i32 = 4;
}

#[derive(Parser)]
#[command(name = "tadpole-mt", version, about = "Meshtastic node for the MeshTadpole SX1262 USB stick. JSON output.")]
struct Cli {
    /// Config file [default: <data dir>/meshtadpole/meshtastic.json; env TADPOLE_MT_CONFIG]
    #[arg(long, global = true, env = "TADPOLE_MT_CONFIG")]
    config: Option<PathBuf>,
    /// SQLite database [default: <data dir>/meshtadpole/meshtastic.db; env TADPOLE_MT_DB]
    #[arg(long, global = true, env = "TADPOLE_MT_DB")]
    db: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List attached MeshTadpole/CH341 sticks.
    Devices,
    /// Show effective config, derived LoRa parameters and paths. Private key is redacted.
    Config {
        /// Write the effective config to the config file.
        #[arg(long)]
        write: bool,
        /// Set a value by dotted path and write the file, e.g. `--set radio.tx_power_dbm=20`.
        /// Value is parsed as JSON, else taken as string. Daemon must be restarted to apply.
        #[arg(long, value_name = "PATH=VALUE")]
        set: Vec<String>,
    },
    /// Our node identity: number, names, public key derived from node.private_key.
    Identity,
    /// Send a text message. Unicast uses PKC when the destination key is known.
    Send {
        /// Destination node id or ^all.
        to: String,
        text: String,
        /// Channel index (channel-encrypted packets).
        #[arg(long, default_value_t = 0)]
        channel: usize,
        /// Force channel encryption for unicast (legacy DM; rejected by firmware >= 2.5).
        #[arg(long)]
        no_pkc: bool,
        /// Do not request an ACK.
        #[arg(long)]
        no_ack: bool,
        #[command(flatten)]
        w: WaitOpts,
    },
    /// Request data from a node: telemetry[:VARIANT], position, traceroute, neighbors, nodeinfo.
    /// Telemetry variants: device (default), local_stats, environment, power, air_quality, health, host.
    Request {
        to: String,
        kind: String,
        #[command(flatten)]
        w: WaitOpts,
    },
    /// Send our NodeInfo (User incl. public key) to a node, requesting theirs.
    Nodeinfo {
        to: String,
        #[command(flatten)]
        w: WaitOpts,
    },
    /// Remote administration over PKC (requires our key in the target's admin_key list).
    Admin {
        /// Target: node id, comma-separated ids, or @group (config `groups`).
        node: String,
        #[command(subcommand)]
        op: AdminOp,
        #[command(flatten)]
        w: WaitOpts,
    },
    /// Run the daemon in the foreground (owns the radio, writes the DB, serves the API).
    Daemon,
    /// Daemon status.
    Status,
    /// Stream received frames from the daemon as NDJSON.
    Listen {
        /// Only frames with status=ok.
        #[arg(long)]
        ok_only: bool,
        /// Exit after N seconds.
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// Query stored packets (v_packets), newest first.
    Packets {
        /// Only frames newer than N seconds ago.
        #[arg(long)]
        since: Option<f64>,
        /// Filter by sender node id (!xxxxxxxx).
        #[arg(long)]
        from: Option<String>,
        /// Filter by port name (e.g. TELEMETRY_APP) or number.
        #[arg(long)]
        port: Option<String>,
        /// Filter by status (ok, crc_error, header_error, no_channel, pkc, decode_error, short).
        #[arg(long)]
        status: Option<String>,
        /// Include duplicate receptions (rebroadcasts of the same packet).
        #[arg(long)]
        dups: bool,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Node summary table, most recently heard first.
    Nodes {
        /// Only nodes heard within N seconds.
        #[arg(long)]
        since: Option<f64>,
    },
    /// One node: summary row plus recent device metrics.
    Node { id: String },
    /// Read-only SQL against the database. Rows as JSON array.
    Sql { query: String },
    /// Print the database schema (DDL).
    Schema,
    /// Decode a raw over-the-air frame given as hex (offline).
    Decode { hex: String },
}

#[derive(clap::Args, Clone, Copy)]
struct WaitOpts {
    /// Seconds to wait for ACK/response per transmission.
    #[arg(long, default_value_t = 60.0, global = true)]
    wait: f64,
    /// Retransmissions if nothing is heard.
    #[arg(long, default_value_t = 1, global = true)]
    retries: u32,
    /// Override hop limit.
    #[arg(long, global = true)]
    hop_limit: Option<u8>,
    /// Admin writes (raw, ignore, unignore): transmit once and return; no ACK/NAK wait, no retransmission.
    /// A session passkey is still required: cached (<140 s) or fetched with one round trip.
    #[arg(long, global = true)]
    no_wait: bool,
}

#[derive(Subcommand)]
enum AdminOp {
    /// Read items: owner, metadata, channel:<0-7>, config:<name>, module:<name>, configs, modules, channels, all.
    /// Config names: device position power network display lora bluetooth security sessionkey deviceui.
    /// Module names: mqtt serial extnotif storeforward rangetest telemetry cannedmsg audio remotehardware
    /// neighborinfo ambientlighting detectionsensor paxcounter statusmessage trafficmanagement tak meshbeacon.
    Get { items: Vec<String> },
    /// Send an arbitrary AdminMessage given as proto3 JSON, e.g. '{"get_owner_request":true}'.
    /// Non-get messages without session_passkey get one from a fresh `config:sessionkey` read.
    Raw {
        json: String,
        /// Non-get messages: request only an ACK (confirms delivery; firmware then discards admin errors).
        /// Default requests a response: admin errors return as NAK, success is silent.
        #[arg(long)]
        ack_only: bool,
    },
    /// Ignore NODE on the target: target drops all packets from NODE (no processing, no rebroadcast).
    /// Uses add_contact{should_ignore}: creates the entry if unknown, never evicted, persisted.
    /// Not readable back over the air; verify via relay_node in rx_frames.
    Ignore { node: String },
    /// Remove NODE from the target's ignore state (remove_ignored_node).
    Unignore { node: String },
    /// Read-modify-write one item (config:<name>, module:<name>, channel:<n>, owner).
    /// CHANGES is a JSON object merged into the current value (objects merge, arrays replace, null = default).
    /// Dry run unless --apply. Security, lora and channel 0 writes also need --allow-disruptive.
    Set {
        item: String,
        changes: String,
        /// Transmit the write. Without it: read, merge, diff only.
        #[arg(long)]
        apply: bool,
        /// Permit writes to security, lora, channel 0.
        #[arg(long)]
        allow_disruptive: bool,
        /// Seconds between write ACK and verification read.
        #[arg(long, default_value_t = 10.0)]
        verify_delay: f64,
    },
}

const CONFIG_TYPES: &[&str] =
    &["device", "position", "power", "network", "display", "lora", "bluetooth", "security", "sessionkey", "deviceui"];
const MODULE_TYPES: &[&str] = &[
    "mqtt",
    "serial",
    "extnotif",
    "storeforward",
    "rangetest",
    "telemetry",
    "cannedmsg",
    "audio",
    "remotehardware",
    "neighborinfo",
    "ambientlighting",
    "detectionsensor",
    "paxcounter",
    "statusmessage",
    "trafficmanagement",
    "tak",
    "meshbeacon",
];

/// Expand admin get item names into (label, AdminMessage JSON).
fn op_label(op: &AdminOp) -> String {
    match op {
        AdminOp::Ignore { node } => format!("ignore:{node}"),
        AdminOp::Unignore { node } => format!("unignore:{node}"),
        _ => "raw".into(),
    }
}

/// AdminMessage add_contact with should_ignore; User from our node table when known.
fn ignore_msg(db_path: &std::path::Path, node: &str) -> Result<Value> {
    let n = packet::parse_node_id(node).context("bad node id")?;
    let mut user = json!({"id": packet::node_id(n)});
    if let Ok(c) = store::open_ro(db_path) {
        if let Some(row) = store::query_json(&c, "SELECT user_json FROM nodes WHERE num = ?1", &[&n])?.into_iter().next() {
            if let Some(u) = row["user_json"].as_object() {
                for k in ["long_name", "short_name", "hw_model"] {
                    if let Some(v) = u.get(k) {
                        user[k] = v.clone();
                    }
                }
            }
        }
    }
    Ok(json!({"add_contact": {"node_num": n, "user": user, "should_ignore": true}}))
}

/// Targets: node id, comma-separated list, or @group (config `groups`). Duplicates removed, order kept.
fn parse_targets(cfg: &Config, spec: &str) -> Result<Vec<u32>> {
    let mut out: Vec<u32> = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let ids: Vec<String> = if let Some(g) = part.strip_prefix('@') {
            cfg.groups.get(g).cloned().ok_or_else(|| anyhow!("unknown group @{g}"))?
        } else {
            vec![part.to_string()]
        };
        for id in ids {
            let n = packet::parse_node_id(&id).ok_or_else(|| anyhow!("bad node id {id}"))?;
            if !out.contains(&n) {
                out.push(n);
            }
        }
    }
    if out.is_empty() {
        bail!("no targets");
    }
    Ok(out)
}

fn admin_items(items: &[String]) -> Result<Vec<(String, Value)>> {
    let mut out = Vec::new();
    for it in items {
        let it = it.to_lowercase();
        match it.as_str() {
            "owner" => out.push((it.clone(), json!({"get_owner_request": true}))),
            "metadata" => out.push((it.clone(), json!({"get_device_metadata_request": true}))),
            "configs" => out.extend(admin_items(
                &CONFIG_TYPES.iter().filter(|c| **c != "sessionkey").map(|c| format!("config:{c}")).collect::<Vec<_>>(),
            )?),
            "modules" => out.extend(admin_items(&MODULE_TYPES.iter().map(|c| format!("module:{c}")).collect::<Vec<_>>())?),
            "channels" => out.extend(admin_items(&(0..8).map(|i| format!("channel:{i}")).collect::<Vec<_>>())?),
            "all" => out.extend(admin_items(&["owner", "metadata", "channels", "configs", "modules"].map(String::from))?),
            _ => {
                let (k, v) = it.split_once(':').ok_or_else(|| anyhow!("unknown item {it}"))?;
                match k {
                    "channel" => {
                        let i: u32 = v.parse()?;
                        out.push((it.clone(), json!({"get_channel_request": i + 1})));
                    }
                    "config" => {
                        CONFIG_TYPES.iter().find(|c| **c == v).ok_or_else(|| anyhow!("unknown config {v}"))?;
                        out.push((it.clone(), json!({"get_config_request": format!("{}_CONFIG", v.to_uppercase())})));
                    }
                    "module" => {
                        MODULE_TYPES.iter().find(|c| **c == v).ok_or_else(|| anyhow!("unknown module {v}"))?;
                        out.push((it.clone(), json!({"get_module_config_request": format!("{}_CONFIG", v.to_uppercase())})));
                    }
                    _ => bail!("unknown item {it}"),
                }
            }
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn send_req(
    to: u32,
    port: &str,
    payload: &[u8],
    want_ack: bool,
    want_response: bool,
    pkc: Option<bool>,
    channel: usize,
    w: WaitOpts,
) -> Value {
    json!({
        "cmd": "send", "to": to, "port": port,
        "payload_b64": base64::engine::general_purpose::STANDARD.encode(payload),
        "want_ack": want_ack, "want_response": want_response, "pkc": pkc, "channel": channel,
        "hop_limit": w.hop_limit, "wait_s": w.wait, "retries": w.retries,
    })
}

/// One admin operation against one target.
fn admin_one(cfg: &Config, db_path: &std::path::Path, to: u32, op: &AdminOp, w: &WaitOpts) -> Result<Value> {
    let items = match op {
        AdminOp::Get { items } => admin_items(items)?,
        AdminOp::Raw { .. } | AdminOp::Ignore { .. } | AdminOp::Unignore { .. } => {
            let mut msg: Value = match op {
                AdminOp::Raw { json: j, .. } => serde_json::from_str(j).context("AdminMessage JSON")?,
                AdminOp::Ignore { node } => ignore_msg(db_path, node)?,
                AdminOp::Unignore { node } => {
                    json!({"remove_ignored_node": packet::parse_node_id(node).context("bad node id")?})
                }
                _ => unreachable!(),
            };
            let is_get = msg.as_object().is_some_and(|m| m.keys().all(|k| k.starts_with("get_") || k == "session_passkey"));
            if !is_get && msg.get("session_passkey").is_none() {
                if let Ok(c) = daemon::call(&cfg.api, &json!({"cmd": "passkey", "to": to})) {
                    if let Some(pk) = c["passkey"].as_str() {
                        msg["session_passkey"] = json!(pk);
                    }
                }
            }
            if !is_get && msg.get("session_passkey").is_none() {
                let payload = proto::encode_json("AdminMessage", &json!({"get_config_request": "SESSIONKEY_CONFIG"}))?;
                let r = pkc_call(cfg, &send_req(to, "ADMIN_APP", &payload, true, true, Some(true), 0, *w), to, *w)?;
                let pk = r["responses"][0]["data"]["payload"]["session_passkey"]
                    .as_str()
                    .ok_or_else(|| anyhow!("could not obtain session_passkey: {}", r["nak"]))?;
                msg["session_passkey"] = json!(pk);
            }
            vec![(op_label(op), msg)]
        }
        AdminOp::Set { item, changes, apply, allow_disruptive, verify_delay } => {
            let items = admin_items(std::slice::from_ref(item))?;
            if items.len() != 1 {
                bail!("set takes exactly one item");
            }
            let changes: Value = serde_json::from_str(changes).context("CHANGES JSON")?;
            let req = json!({
                "cmd": "admin_set", "to": to, "get": items[0].1, "changes": changes,
                "apply": apply, "allow_disruptive": allow_disruptive, "verify_delay_s": verify_delay,
                "wait_s": w.wait, "retries": w.retries, "hop_limit": w.hop_limit,
            });
            let r = match daemon::call(&cfg.api, &req) {
                Err(e) if format!("{e:#}").contains("PKI_UNKNOWN_PUBKEY") => {
                    nodeinfo_exchange(cfg, to, *w)?;
                    daemon::call(&cfg.api, &req)?
                }
                r => r?,
            };
            return Ok(r);
        }
    };
    let mut results = Vec::new();
    for (label, msg) in items {
        let payload = proto::encode_json("AdminMessage", &msg)?;
        // get_* expect a response; other messages (set_*, reboot, ...) only an ACK.
        let is_get = msg.as_object().is_some_and(|m| m.keys().any(|k| k.starts_with("get_")));
        let ack_only = matches!(op, AdminOp::Raw { ack_only: true, .. });
        let fire = w.no_wait && !is_get;
        let r = if fire {
            let wf = WaitOpts { wait: 0.0, retries: 0, ..*w };
            daemon::call(&cfg.api, &send_req(to, "ADMIN_APP", &payload, false, false, Some(true), 0, wf))
        } else {
            pkc_call(cfg, &send_req(to, "ADMIN_APP", &payload, true, !ack_only, Some(true), 0, *w), to, *w)
        };
        let entry = match r {
            Ok(r) => {
                let resp = r["responses"].as_array().and_then(|a| a.first()).map(|e| e["data"]["payload"].clone());
                // Non-get: ack_only -> ok = ACK; else ok = no NAK (silence = not rejected; delivery unconfirmed).
                let ok = if fire {
                    r["transmissions"].as_array().is_some_and(|a| !a.is_empty())
                } else if is_get {
                    resp.is_some()
                } else if ack_only {
                    r["acked"].as_bool().unwrap_or(false) && r["nak"].is_null()
                } else {
                    r["nak"].is_null()
                };
                json!({
                    "item": label, "ok": ok, "fire_and_forget": fire, "acked": r["acked"], "response": resp, "nak": r["nak"],
                    "packet_id": r["packet_id"], "transmissions": r["transmissions"].as_array().map(|a| a.len()),
                    "relays_heard": r["relays_heard"].as_array().map(|a| a.len()),
                    "key_exchange": r.get("key_exchange"),
                    "response_rf": r["responses"].as_array().and_then(|a| a.first()).map(|e| json!({"rssi": e["rssi"], "snr": e["snr"], "hops_taken": e["hops_taken"], "relay_node": e["relay_node"]})),
                })
            }
            Err(e) => json!({"item": label, "ok": false, "error": format!("{e:#}")}),
        };
        eprintln!("{}", json!({"progress": entry["item"], "ok": entry["ok"]}));
        results.push(entry);
    }
    Ok(json!({"node": packet::node_id(to), "results": results}))
}

/// Send our NodeInfo to `to` over the channel, requesting theirs.
fn nodeinfo_exchange(cfg: &Config, to: u32, w: WaitOpts) -> Result<Value> {
    let me = cfg.node.num.ok_or_else(|| anyhow!("node.num not set"))?;
    let pubk = cfg.node.private_key.as_deref().map(pkc::Identity::from_b64).transpose()?.map(|i| i.public_b64());
    let payload = mesh::user_payload(cfg, me, pubk)?;
    daemon::call(&cfg.api, &send_req(to, "NODEINFO_APP", &payload, false, true, Some(false), 0, w))
}

/// PKC send; on PKI_UNKNOWN_PUBKEY NAK, exchange NodeInfo and retry once.
fn pkc_call(cfg: &Config, req: &Value, to: u32, w: WaitOpts) -> Result<Value> {
    let r = daemon::call(&cfg.api, req)?;
    if r["nak"] != "PKI_UNKNOWN_PUBKEY" {
        return Ok(r);
    }
    let ni = nodeinfo_exchange(cfg, to, w)?;
    let mut r2 = daemon::call(&cfg.api, req)?;
    r2["key_exchange"] = json!({"first_packet_id": r["packet_id"], "nak": r["nak"], "nodeinfo_acked": ni["acked"]});
    Ok(r2)
}

/// SNR as carried in RouteDiscovery (dB * 4, i8; -128 = unknown).
fn route_snr(v: &Value) -> Value {
    match v.as_i64() {
        Some(-128) | None => Value::Null,
        Some(x) => json!(x as f64 / 4.0),
    }
}

/// Hop list `[{node, snr_db}]`: `start` -> route... -> `end`; snr[i] is measured by the receiver of hop i.
fn route_hops(start: u32, route: &Value, snrs: &Value, end: u32) -> Value {
    let mut nodes = vec![start];
    nodes.extend(route.as_array().into_iter().flatten().filter_map(|x| x.as_u64()).map(|x| x as u32));
    nodes.push(end);
    let snr: Vec<Value> = snrs.as_array().map(|a| a.iter().map(route_snr).collect()).unwrap_or_default();
    json!(nodes
        .windows(2)
        .enumerate()
        .map(|(i, w)| json!({"from": packet::node_id(w[0]), "to": packet::node_id(w[1]), "snr_db": snr.get(i).cloned().unwrap_or(Value::Null)}))
        .collect::<Vec<_>>())
}

fn request(cfg: &Config, to: u32, kind: &str, w: WaitOpts) -> Result<Value> {
    let (k, variant) = kind.split_once(':').map(|(a, b)| (a, Some(b))).unwrap_or((kind, None));
    // Firmware never uses PKC for TRACEROUTE, POSITION, NODEINFO, ROUTING (relays must read traceroutes).
    let (port, payload, pkc): (&str, Vec<u8>, Option<bool>) = match k {
        "telemetry" => {
            let v = match variant.unwrap_or("device") {
                "device" => "device_metrics",
                "local_stats" => "local_stats",
                "environment" => "environment_metrics",
                "power" => "power_metrics",
                "air_quality" => "air_quality_metrics",
                "health" => "health_metrics",
                "host" => "host_metrics",
                x => bail!("unknown telemetry variant {x}"),
            };
            ("TELEMETRY_APP", proto::encode_json("Telemetry", &json!({ v: {} }))?, None)
        }
        "position" => ("POSITION_APP", proto::encode_json("Position", &json!({}))?, Some(false)),
        "traceroute" => ("TRACEROUTE_APP", proto::encode_json("RouteDiscovery", &json!({}))?, Some(false)),
        "neighbors" | "neighborinfo" => ("NEIGHBORINFO_APP", proto::encode_json("NeighborInfo", &json!({}))?, Some(false)),
        "nodeinfo" => return nodeinfo_exchange(cfg, to, w),
        x => bail!("unknown request kind {x}"),
    };
    // PKC requests set want_ack: firmware sends PKI_UNKNOWN_PUBKEY only for want_ack packets.
    let req = send_req(to, port, &payload, pkc != Some(false), true, pkc, 0, w);
    let mut r = if pkc == Some(false) { daemon::call(&cfg.api, &req)? } else { pkc_call(cfg, &req, to, w)? };
    // No response and no NAK. Observed: the first request after an idle period goes unanswered while an
    // immediate repeat succeeds (likely a NeighborInfo reply lost or suppressed, see below). So: repeat once;
    // if still nothing on a PKC request, the NAK may have been lost -> exchange NodeInfo and try once more.
    let empty = |r: &Value| r["responses"].as_array().is_none_or(|a| a.is_empty()) && r["nak"].is_null();
    let mut unanswered = Vec::new();
    if empty(&r) && r.get("key_exchange").is_none() {
        unanswered.push(r["packet_id"].clone());
        r = daemon::call(&cfg.api, &req)?;
        if pkc != Some(false) && r["encryption"] == "pkc" && empty(&r) {
            unanswered.push(r["packet_id"].clone());
            let ni = nodeinfo_exchange(cfg, to, w)?;
            r = daemon::call(&cfg.api, &req)?;
            r["key_exchange"] = json!({"reason": "no response after repeat", "nodeinfo_acked": ni["acked"]});
        }
    }
    // Firmware bug: with NeighborInfo enabled, its wantPacket() accepts every packet and it is registered
    // before the telemetry modules, so it answers want_response requests on other ports. Its reply is
    // throttled to 1 per 3 min; an immediate repeat therefore reaches the intended module.
    let mut incidental = Vec::new();
    let first_port = r["responses"][0]["port"].clone();
    if first_port == "NEIGHBORINFO_APP" && port != "NEIGHBORINFO_APP" {
        incidental.push(json!({"packet_id": r["packet_id"], "port": first_port, "payload": r["responses"][0]["data"]["payload"]}));
        r = daemon::call(&cfg.api, &req)?;
    }
    let resp = r["responses"].as_array().and_then(|a| a.first()).cloned();
    let mut out = json!({
        "to": packet::node_id(to), "kind": kind, "packet_id": r["packet_id"], "encryption": r["encryption"],
        // A node may answer with another module's reply (observed: NEIGHBORINFO_APP to a TELEMETRY_APP request).
        "ok": resp.as_ref().is_some_and(|e| e["port"] == port),
        "response_port": resp.as_ref().map(|e| e["port"].clone()),
        "nak": r["nak"],
        "key_exchange": r.get("key_exchange"),
        "incidental_responses": incidental,
        "unanswered_packet_ids": unanswered,
        "transmissions": r["transmissions"].as_array().map(|a| a.len()),
        "relays_heard": r["relays_heard"],
        "response": resp.as_ref().map(|e| e["data"]["payload"].clone()),
        "response_rf": resp.as_ref().map(|e| json!({"rssi": e["rssi"], "snr": e["snr"], "hops_taken": e["hops_taken"], "hop_start": e["hop_start"], "relay_node": e["relay_node"], "rx_time": e["rx_time"], "frame_id": e["frame_id"]})),
    });
    if r["nak"] == "NO_RESPONSE" {
        out["note"] = json!(
            "node has no handler for this request (module disabled, variant unsupported, or reply throttled: position/neighbors 1 per 3 min)"
        );
    }
    if k == "traceroute" {
        if let Some(e) = &resp {
            let me = cfg.node.num.unwrap_or(0);
            let p = &e["data"]["payload"];
            let mut back_snr = p["snr_back"].clone();
            // The originator measures the last hop of the return path itself.
            if let Some(a) = back_snr.as_array_mut() {
                a.push(json!((e["snr"].as_f64().unwrap_or(0.0) * 4.0).round() as i64));
            } else {
                back_snr = json!([(e["snr"].as_f64().unwrap_or(0.0) * 4.0).round() as i64]);
            }
            out["forward"] = route_hops(me, &p["route"], &p["snr_towards"], to);
            out["back"] = route_hops(to, &p["route_back"], &back_snr, me);
        }
    }
    Ok(out)
}

fn set_path(v: &mut Value, path: &str, val: Value) -> Result<()> {
    let mut cur = v;
    let parts: Vec<&str> = path.split('.').collect();
    for p in &parts[..parts.len() - 1] {
        cur = cur.get_mut(*p).ok_or_else(|| anyhow!("no such key {p} in {path}"))?;
    }
    let last = parts[parts.len() - 1];
    let obj = cur.as_object_mut().ok_or_else(|| anyhow!("{path}: parent is not an object"))?;
    // `groups.<name>` may be created; other keys must exist.
    if !obj.contains_key(last) && !(parts.len() == 2 && parts[0] == "groups") {
        bail!("no such key {last} in {path}");
    }
    obj.insert(last.into(), val);
    Ok(())
}

fn paths(cli: &Cli) -> (PathBuf, PathBuf) {
    let d = config::default_dir();
    (cli.config.clone().unwrap_or_else(|| d.join("meshtastic.json")), cli.db.clone().unwrap_or_else(|| d.join("meshtastic.db")))
}

fn print(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap());
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(&cli) {
        let msg = format!("{e:#}");
        eprintln!("{}", json!({"error": msg}));
        let code = if msg.contains("daemon not reachable") {
            exit::NO_DAEMON
        } else if msg.contains("device not found") {
            exit::NO_DEVICE
        } else {
            exit::ERROR
        };
        std::process::exit(code);
    }
}

fn run(cli: &Cli) -> Result<()> {
    let (cfg_path, db_path) = paths(cli);
    let cfg = Config::load(&cfg_path)?;
    decode::set_redact_fields(cfg.redact_fields.clone());
    match &cli.cmd {
        Cmd::Devices => {
            let list = meshtadpole_radio::ch341::list(meshtadpole_radio::ch341::VID, meshtadpole_radio::ch341::PID)?;
            let v: Vec<Value> = list
                .iter()
                .map(|d| {
                    json!({
                        "bus": d.bus, "address": d.address, "port_path": d.port_path,
                        "vid": format!("{:04x}", d.vid), "pid": format!("{:04x}", d.pid),
                        "product": d.product, "serial": d.serial, "manufacturer": d.manufacturer,
                        "device_id": meshtadpole_radio::ch341::device_id(&d.product_raw).map(hex::encode),
                        "open_error": d.open_error,
                    })
                })
                .collect();
            print(&json!(v));
        }
        Cmd::Config { write, set } => {
            let mut cfg = cfg.clone();
            if !set.is_empty() {
                let mut v = serde_json::to_value(&cfg)?;
                for s in set {
                    let (p, val) = s.split_once('=').ok_or_else(|| anyhow!("expected PATH=VALUE: {s}"))?;
                    let val = if p == "node.num" {
                        json!(packet::parse_node_id(val).ok_or_else(|| anyhow!("bad node id {val}"))?)
                    } else {
                        serde_json::from_str(val).unwrap_or(Value::String(val.to_string()))
                    };
                    set_path(&mut v, p, val)?;
                }
                cfg = serde_json::from_value(v).context("invalid config after set")?;
                cfg.lora()?;
                cfg.channels()?;
                if let Some(k) = &cfg.node.private_key {
                    pkc::Identity::from_b64(k)?;
                }
            }
            if *write || !set.is_empty() {
                cfg.save(&cfg_path)?;
            }
            let cfg = daemon::redacted(&cfg);
            let lora = cfg.lora()?;
            let chans: Vec<Value> =
                cfg.channels()?.iter().map(|c| json!({"name": c.name, "hash": c.hash, "key_bits": c.key.len() * 8})).collect();
            print(&json!({
                "config_path": cfg_path, "config_exists": cfg_path.exists(), "db_path": db_path,
                "config": cfg,
                "derived": {
                    "freq_hz": lora.freq_hz, "bw_hz": lora.bw_hz, "sf": lora.sf, "cr": format!("4/{}", lora.cr),
                    "sync_word": format!("0x{:02x}", lora.sync_word), "preamble_len": lora.preamble_len,
                    "ldro": lora.ldro(), "symbol_ms": lora.symbol_us() / 1000.0,
                    "channels": chans,
                }
            }));
        }
        Cmd::Identity => {
            let id = cfg.node.private_key.as_deref().map(pkc::Identity::from_b64).transpose()?;
            print(&json!({
                "num": cfg.node.num, "id": cfg.node.num.map(packet::node_id),
                "long_name": cfg.node.long_name, "short_name": cfg.node.short_name,
                "role": cfg.node.role, "hw_model": cfg.node.hw_model, "hop_limit": cfg.node.hop_limit,
                "public_key": id.map(|i| i.public_b64()),
            }));
        }
        Cmd::Send { to, text, channel, no_pkc, no_ack, w } => {
            let to = packet::parse_node_id(to).context("bad node id")?;
            let pkc = if *no_pkc { Some(false) } else { None };
            let req = send_req(to, "TEXT_MESSAGE_APP", text.as_bytes(), !no_ack && to != packet::BROADCAST, false, pkc, *channel, *w);
            let r = if to != packet::BROADCAST && !no_pkc { pkc_call(&cfg, &req, to, *w)? } else { daemon::call(&cfg.api, &req)? };
            print(&r);
        }
        Cmd::Request { to, kind, w } => {
            let to = packet::parse_node_id(to).context("bad node id")?;
            print(&request(&cfg, to, kind, *w)?);
        }
        Cmd::Nodeinfo { to, w } => {
            let to = packet::parse_node_id(to).context("bad node id")?;
            print(&nodeinfo_exchange(&cfg, to, *w)?);
        }
        Cmd::Admin { node, op, w } => {
            let targets = parse_targets(&cfg, node)?;
            if targets.len() == 1 {
                print(&admin_one(&cfg, &db_path, targets[0], op, w)?);
            } else {
                let mut out = Vec::new();
                for t in targets {
                    eprintln!("{}", json!({"target": packet::node_id(t)}));
                    out.push(match admin_one(&cfg, &db_path, t, op, w) {
                        Ok(v) => json!({"node": packet::node_id(t), "ok": true, "result": v}),
                        Err(e) => json!({"node": packet::node_id(t), "ok": false, "error": format!("{e:#}")}),
                    });
                }
                print(&json!({"targets": out}));
            }
        }
        Cmd::Daemon => daemon::run(&cfg, &db_path)?,
        Cmd::Status => {
            let mut s = daemon::connect(&cfg.api)?;
            writeln!(s, "{}", json!({"cmd": "status"}))?;
            let mut line = String::new();
            BufReader::new(s).read_line(&mut line)?;
            print(&serde_json::from_str(&line)?);
        }
        Cmd::Listen { ok_only, seconds } => {
            let mut s = daemon::connect(&cfg.api)?;
            if let Some(sec) = seconds {
                let sec = *sec;
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_secs(sec));
                    std::process::exit(0);
                });
            }
            writeln!(s, "{}", json!({"cmd": "subscribe"}))?;
            let out = std::io::stdout();
            for line in BufReader::new(s).lines() {
                let line = line?;
                let v: Value = serde_json::from_str(&line)?;
                if v.get("subscribed").is_some() {
                    continue;
                }
                if *ok_only && v["status"] != "ok" {
                    continue;
                }
                let mut o = out.lock();
                writeln!(o, "{line}")?;
                o.flush()?;
            }
        }
        Cmd::Packets { since, from, port, status, dups, limit } => {
            let c = store::open_ro(&db_path)?;
            let mut q = String::from("SELECT * FROM v_packets WHERE 1=1");
            let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            if let Some(s) = since {
                q += " AND rx_time > ?";
                args.push(Box::new(store::now() - s));
            }
            if let Some(f) = from {
                let n = packet::parse_node_id(f).context("bad node id")?;
                q += " AND from_id = ?";
                args.push(Box::new(format!("!{n:08x}")));
            }
            if let Some(p) = port {
                if let Ok(n) = p.parse::<i64>() {
                    q += " AND id IN (SELECT id FROM rx_frames WHERE portnum = ?)";
                    args.push(Box::new(n));
                } else {
                    q += " AND port = ?";
                    args.push(Box::new(p.to_uppercase()));
                }
            }
            if let Some(s) = status {
                q += " AND status = ?";
                args.push(Box::new(s.clone()));
            }
            if !dups {
                q += " AND dup_of IS NULL";
            }
            q += " ORDER BY id DESC LIMIT ?";
            args.push(Box::new(*limit));
            let refs: Vec<&dyn rusqlite::ToSql> = args.iter().map(|b| b.as_ref()).collect();
            print(&json!(store::query_json(&c, &q, &refs)?));
        }
        Cmd::Nodes { since } => {
            let c = store::open_ro(&db_path)?;
            let cutoff = since.map(|s| store::now() - s).unwrap_or(0.0);
            let q = "SELECT num, id, long_name, short_name, hw_model, role, packets_heard,
                       first_heard, last_heard, CAST(strftime('%s','now') AS REAL) - last_heard AS age_s,
                       last_rssi, last_snr, last_hops_taken, last_relay_node, last_via_mqtt,
                       last_direct_heard, last_direct_rssi, last_direct_snr,
                       latitude, longitude, altitude, position_time,
                       battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds, device_metrics_time,
                       public_key
                     FROM nodes WHERE last_heard > ?1 ORDER BY last_heard DESC";
            print(&json!(store::query_json(&c, q, &[&cutoff])?));
        }
        Cmd::Node { id } => {
            let c = store::open_ro(&db_path)?;
            let n = packet::parse_node_id(id).context("bad node id")?;
            let node = store::query_json(&c, "SELECT * FROM nodes WHERE num = ?1", &[&n])?;
            let metrics =
                store::query_json(&c, "SELECT * FROM v_device_metrics WHERE node_num = ?1 ORDER BY rx_time DESC LIMIT 50", &[&n])?;
            print(&json!({"node": node.into_iter().next(), "device_metrics": metrics}));
        }
        Cmd::Sql { query } => {
            let c = store::open_ro(&db_path)?;
            print(&json!(store::query_json(&c, query, &[])?));
        }
        Cmd::Schema => println!("{}", store::full_schema()),
        Cmd::Decode { hex: h } => {
            let raw = hex::decode(h.trim()).context("hex")?;
            let d = decode::decode_frame(&raw, false, &cfg.channels()?, None);
            print(&decode::to_json(&d));
        }
    }
    Ok(())
}
