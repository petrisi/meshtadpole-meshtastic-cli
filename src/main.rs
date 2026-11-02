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
    /// Send our NodeInfo (User incl. public key) to a node, requesting theirs.
    Nodeinfo {
        to: String,
        #[command(flatten)]
        w: WaitOpts,
    },
    /// Remote administration over PKC (requires our key in the target's admin_key list).
    Admin {
        /// Target node id.
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
    #[arg(long, default_value_t = 60.0)]
    wait: f64,
    /// Retransmissions if nothing is heard.
    #[arg(long, default_value_t = 1)]
    retries: u32,
    /// Override hop limit.
    #[arg(long)]
    hop_limit: Option<u8>,
}

#[derive(Subcommand)]
enum AdminOp {
    /// Read items: owner, metadata, channel:<0-7>, config:<name>, module:<name>, configs, modules, channels, all.
    /// Config names: device position power network display lora bluetooth security sessionkey deviceui.
    /// Module names: mqtt serial extnotif storeforward rangetest telemetry cannedmsg audio remotehardware
    /// neighborinfo ambientlighting detectionsensor paxcounter statusmessage trafficmanagement tak meshbeacon.
    Get { items: Vec<String> },
    /// Send an arbitrary AdminMessage given as proto3 JSON, e.g. '{"get_owner_request":true}'.
    Raw { json: String },
}

const CONFIG_TYPES: &[&str] = &["device", "position", "power", "network", "display", "lora", "bluetooth", "security", "sessionkey", "deviceui"];
const MODULE_TYPES: &[&str] = &[
    "mqtt", "serial", "extnotif", "storeforward", "rangetest", "telemetry", "cannedmsg", "audio", "remotehardware",
    "neighborinfo", "ambientlighting", "detectionsensor", "paxcounter", "statusmessage", "trafficmanagement", "tak", "meshbeacon",
];

/// Expand admin get item names into (label, AdminMessage JSON).
fn admin_items(items: &[String]) -> Result<Vec<(String, Value)>> {
    let mut out = Vec::new();
    for it in items {
        let it = it.to_lowercase();
        match it.as_str() {
            "owner" => out.push((it.clone(), json!({"get_owner_request": true}))),
            "metadata" => out.push((it.clone(), json!({"get_device_metadata_request": true}))),
            "configs" => out.extend(admin_items(&CONFIG_TYPES.iter().filter(|c| **c != "sessionkey").map(|c| format!("config:{c}")).collect::<Vec<_>>())?),
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

fn send_req(to: u32, port: &str, payload: &[u8], want_ack: bool, want_response: bool, pkc: Option<bool>, channel: usize, w: WaitOpts) -> Value {
    json!({
        "cmd": "send", "to": to, "port": port,
        "payload_b64": base64::engine::general_purpose::STANDARD.encode(payload),
        "want_ack": want_ack, "want_response": want_response, "pkc": pkc, "channel": channel,
        "hop_limit": w.hop_limit, "wait_s": w.wait, "retries": w.retries,
    })
}

fn set_path(v: &mut Value, path: &str, val: Value) -> Result<()> {
    let mut cur = v;
    let parts: Vec<&str> = path.split('.').collect();
    for p in &parts[..parts.len() - 1] {
        cur = cur.get_mut(*p).ok_or_else(|| anyhow!("no such key {p} in {path}"))?;
    }
    let last = parts[parts.len() - 1];
    let obj = cur.as_object_mut().ok_or_else(|| anyhow!("{path}: parent is not an object"))?;
    if !obj.contains_key(last) {
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
                    let val = serde_json::from_str(val).unwrap_or(Value::String(val.to_string()));
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
            let chans: Vec<Value> = cfg
                .channels()?
                .iter()
                .map(|c| json!({"name": c.name, "hash": c.hash, "key_bits": c.key.len() * 8}))
                .collect();
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
            let r = daemon::call(&cfg.api, &send_req(to, "TEXT_MESSAGE_APP", text.as_bytes(), !no_ack && to != packet::BROADCAST, false, pkc, *channel, *w))?;
            print(&r);
        }
        Cmd::Nodeinfo { to, w } => {
            let to = packet::parse_node_id(to).context("bad node id")?;
            let me = cfg.node.num.ok_or_else(|| anyhow!("node.num not set"))?;
            let pubk = cfg.node.private_key.as_deref().map(pkc::Identity::from_b64).transpose()?.map(|i| i.public_b64());
            let payload = mesh::user_payload(&cfg, me, pubk)?;
            // NodeInfo exchange goes over the channel (receiver may not know our key yet).
            let r = daemon::call(&cfg.api, &send_req(to, "NODEINFO_APP", &payload, false, true, Some(false), 0, *w))?;
            print(&r);
        }
        Cmd::Admin { node, op, w } => {
            let to = packet::parse_node_id(node).context("bad node id")?;
            let items = match op {
                AdminOp::Get { items } => admin_items(items)?,
                AdminOp::Raw { json: j } => vec![("raw".into(), serde_json::from_str(j).context("AdminMessage JSON")?)],
            };
            let mut results = Vec::new();
            for (label, msg) in items {
                let payload = proto::encode_json("AdminMessage", &msg)?;
                let r = daemon::call(&cfg.api, &send_req(to, "ADMIN_APP", &payload, true, true, Some(true), 0, *w));
                let entry = match r {
                    Ok(r) => {
                        let resp = r["responses"].as_array().and_then(|a| a.first()).map(|e| e["data"]["payload"].clone());
                        json!({
                            "item": label, "ok": resp.is_some(), "response": resp, "nak": r["nak"],
                            "packet_id": r["packet_id"], "transmissions": r["transmissions"].as_array().map(|a| a.len()),
                            "relays_heard": r["relays_heard"].as_array().map(|a| a.len()),
                            "response_rf": r["responses"].as_array().and_then(|a| a.first()).map(|e| json!({"rssi": e["rssi"], "snr": e["snr"], "hops_taken": e["hops_taken"], "relay_node": e["relay_node"]})),
                        })
                    }
                    Err(e) => json!({"item": label, "ok": false, "error": format!("{e:#}")}),
                };
                eprintln!("{}", json!({"progress": entry["item"], "ok": entry["ok"]}));
                results.push(entry);
            }
            print(&json!({"node": packet::node_id(to), "results": results}));
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
            let metrics = store::query_json(
                &c,
                "SELECT * FROM v_device_metrics WHERE node_num = ?1 ORDER BY rx_time DESC LIMIT 50",
                &[&n],
            )?;
            print(&json!({"node": node.into_iter().next(), "device_metrics": metrics}));
        }
        Cmd::Sql { query } => {
            let c = store::open_ro(&db_path)?;
            print(&json!(store::query_json(&c, query, &[])?));
        }
        Cmd::Schema => println!("{}", store::SCHEMA.trim()),
        Cmd::Decode { hex: h } => {
            let raw = hex::decode(h.trim()).context("hex")?;
            let d = decode::decode_frame(&raw, false, &cfg.channels()?, None);
            print(&decode::to_json(&d));
        }
    }
    Ok(())
}
