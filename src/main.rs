mod channel;
mod config;
mod daemon;
mod decode;
mod packet;
mod proto;
mod store;

use anyhow::{Context, Result};
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
    /// Show effective config, derived LoRa parameters and paths.
    Config {
        /// Write the effective config to the config file.
        #[arg(long)]
        write: bool,
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
        Cmd::Config { write } => {
            if *write {
                cfg.save(&cfg_path)?;
            }
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
            let d = decode::decode_frame(&raw, false, &cfg.channels()?);
            print(&decode::to_json(&d));
        }
    }
    Ok(())
}
