//! SQLite store. Schema is part of the public interface (see docs/schema.md).

use crate::decode::{Decoded, Status};
use anyhow::{Result, anyhow};
use meshtadpole_radio::sx1262::RxFrame;
use meshtadpole_radio::store::Session;
use rusqlite::{Connection, params};
use serde_json::Value;
use std::path::Path;

pub const SCHEMA_VERSION: i64 = 3;

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS rx_frames(
  id INTEGER PRIMARY KEY,
  session_id INTEGER,
  rx_time REAL NOT NULL,
  raw BLOB,
  len INTEGER,
  airtime_ms REAL,
  rssi REAL, snr REAL, signal_rssi REAL, freq_error_hz REAL,
  status TEXT NOT NULL,
  pkt_to INTEGER, pkt_from INTEGER, pkt_id INTEGER,
  hop_limit INTEGER, hop_start INTEGER, hops_taken INTEGER,
  want_ack INTEGER, via_mqtt INTEGER,
  channel_hash INTEGER, next_hop INTEGER, relay_node INTEGER,
  channel TEXT, portnum INTEGER, port TEXT,
  data_json TEXT,
  error TEXT,
  dup_of INTEGER
);
CREATE INDEX IF NOT EXISTS rx_frames_time ON rx_frames(rx_time);
CREATE INDEX IF NOT EXISTS rx_frames_from_time ON rx_frames(pkt_from, rx_time);
CREATE INDEX IF NOT EXISTS rx_frames_from_id ON rx_frames(pkt_from, pkt_id);
CREATE INDEX IF NOT EXISTS rx_frames_port_time ON rx_frames(portnum, rx_time);

CREATE TABLE IF NOT EXISTS nodes(
  num INTEGER PRIMARY KEY,
  id TEXT NOT NULL,
  long_name TEXT, short_name TEXT, hw_model TEXT, role TEXT, public_key TEXT, is_licensed INTEGER,
  user_json TEXT, user_time REAL,
  first_heard REAL, last_heard REAL, last_frame_id INTEGER, packets_heard INTEGER NOT NULL DEFAULT 0,
  last_rssi REAL, last_snr REAL, last_hops_taken INTEGER, last_relay_node INTEGER, last_via_mqtt INTEGER,
  last_direct_heard REAL, last_direct_rssi REAL, last_direct_snr REAL,
  position_json TEXT, position_time REAL, latitude REAL, longitude REAL, altitude INTEGER,
  device_metrics_json TEXT, device_metrics_time REAL,
  battery_level INTEGER, voltage REAL, channel_utilization REAL, air_util_tx REAL, uptime_seconds INTEGER
);

CREATE TABLE IF NOT EXISTS tx_frames(
  id INTEGER PRIMARY KEY,
  session_id INTEGER,
  tx_time REAL NOT NULL,
  raw BLOB, len INTEGER, airtime_ms REAL,
  pkt_to INTEGER, pkt_id INTEGER, hop_limit INTEGER, want_ack INTEGER, channel_hash INTEGER,
  encryption TEXT, portnum INTEGER, port TEXT, data_json TEXT,
  attempt INTEGER, error TEXT
);
CREATE INDEX IF NOT EXISTS tx_frames_time ON tx_frames(tx_time);
CREATE INDEX IF NOT EXISTS tx_frames_id ON tx_frames(pkt_id);

CREATE VIEW IF NOT EXISTS v_packets AS
SELECT id, rx_time, datetime(rx_time,'unixepoch') AS rx_utc, status,
  printf('!%08x', pkt_from) AS from_id, CASE WHEN pkt_to=4294967295 THEN '^all' ELSE printf('!%08x', pkt_to) END AS to_id,
  pkt_id, hop_start, hop_limit, hops_taken, relay_node, next_hop, want_ack, via_mqtt,
  rssi, snr, len, airtime_ms, channel, port, data_json, dup_of
FROM rx_frames;

CREATE VIEW IF NOT EXISTS v_device_metrics AS
SELECT id AS frame_id, rx_time, pkt_from AS node_num, printf('!%08x', pkt_from) AS node_id,
  json_extract(data_json,'$.payload.time') AS reported_time,
  json_extract(data_json,'$.payload.device_metrics.battery_level') AS battery_level,
  json_extract(data_json,'$.payload.device_metrics.voltage') AS voltage,
  json_extract(data_json,'$.payload.device_metrics.channel_utilization') AS channel_utilization,
  json_extract(data_json,'$.payload.device_metrics.air_util_tx') AS air_util_tx,
  json_extract(data_json,'$.payload.device_metrics.uptime_seconds') AS uptime_seconds
FROM rx_frames
WHERE status='ok' AND portnum=67 AND dup_of IS NULL
  AND json_extract(data_json,'$.payload.device_metrics') IS NOT NULL;

CREATE VIEW IF NOT EXISTS v_positions AS
SELECT id AS frame_id, rx_time, pkt_from AS node_num, printf('!%08x', pkt_from) AS node_id,
  json_extract(data_json,'$.payload.latitude_i') * 1e-7 AS latitude,
  json_extract(data_json,'$.payload.longitude_i') * 1e-7 AS longitude,
  json_extract(data_json,'$.payload.altitude') AS altitude,
  json_extract(data_json,'$.payload.time') AS reported_time,
  json_extract(data_json,'$.payload.precision_bits') AS precision_bits
FROM rx_frames
WHERE status='ok' AND portnum=3 AND dup_of IS NULL;

CREATE VIEW IF NOT EXISTS v_text AS
SELECT id AS frame_id, rx_time, printf('!%08x', pkt_from) AS from_id,
  CASE WHEN pkt_to=4294967295 THEN '^all' ELSE printf('!%08x', pkt_to) END AS to_id,
  channel, json_extract(data_json,'$.payload') AS text
FROM rx_frames
WHERE status='ok' AND portnum=1 AND dup_of IS NULL;
"#;

pub use meshtadpole_radio::store::{COMMON_SCHEMA, now};

/// Full DDL (common tables from meshtadpole-radio + tool tables).
pub fn full_schema() -> String {
    format!("{}
{}", COMMON_SCHEMA.trim(), SCHEMA.trim())
}

pub fn open_rw(path: &Path) -> Result<Connection> {
    meshtadpole_radio::store::open_rw(path, SCHEMA, SCHEMA_VERSION).map_err(|e| anyhow!(e))
}

pub fn open_ro(path: &Path) -> Result<Connection> {
    meshtadpole_radio::store::open_ro(path).map_err(|e| anyhow!(e))
}

/// Run a query, rows as JSON objects. BLOBs become lowercase hex.
pub fn query_json(c: &Connection, sql: &str, args: &[&dyn rusqlite::ToSql]) -> Result<Vec<Value>> {
    Ok(meshtadpole_radio::store::query_json(c, sql, args)?)
}

pub struct Writer {
    pub s: Session,
    pub session_id: i64,
}

impl Writer {
    pub fn start_session(c: Connection, usb_serial: &str, lora: &meshtadpole_radio::sx1262::LoraConfig, cfg_json: &str) -> Result<Writer> {
        let s = Session::start(c, env!("CARGO_PKG_VERSION"), usb_serial, lora, cfg_json)?;
        let session_id = s.id;
        Ok(Writer { s, session_id })
    }

    pub fn insert_header_error(&self, t: f64, rssi_inst: f32) -> Result<i64> {
        self.s.c.execute(
            "INSERT INTO rx_frames(session_id,rx_time,rssi,status) VALUES(?1,?2,?3,'header_error')",
            params![self.session_id, t, rssi_inst],
        )?;
        Ok(self.s.c.last_insert_rowid())
    }

    /// Insert a frame and update the node summary. Returns (frame id, dup_of).
    pub fn insert_frame(&self, t: f64, f: &RxFrame, airtime_ms: f64, d: &Decoded) -> Result<(i64, Option<i64>)> {
        let h = d.header;
        let trusted = d.status != Status::CrcError && h.is_some();
        let dup_of: Option<i64> = if trusted {
            let h = h.unwrap();
            self.s.c
                .query_row(
                    "SELECT id FROM rx_frames WHERE pkt_from=?1 AND pkt_id=?2 AND dup_of IS NULL AND status!='crc_error' AND rx_time>?3 ORDER BY id LIMIT 1",
                    params![h.from, h.id, t - 3600.0],
                    |r| r.get(0),
                )
                .ok()
        } else {
            None
        };
        let status = serde_json::to_value(d.status)?.as_str().unwrap().to_string();
        self.s.c.execute(
            "INSERT INTO rx_frames(session_id,rx_time,raw,len,airtime_ms,rssi,snr,signal_rssi,freq_error_hz,status,
               pkt_to,pkt_from,pkt_id,hop_limit,hop_start,hops_taken,want_ack,via_mqtt,channel_hash,next_hop,relay_node,
               channel,portnum,port,data_json,error,dup_of)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27)",
            params![
                self.session_id, t, f.data, f.data.len(), airtime_ms, f.rssi_dbm, f.snr_db, f.signal_rssi_dbm, f.freq_error_hz, status,
                h.map(|h| h.to), h.map(|h| h.from), h.map(|h| h.id), h.map(|h| h.hop_limit), h.map(|h| h.hop_start),
                h.and_then(|h| h.hops_taken()), h.map(|h| h.want_ack), h.map(|h| h.via_mqtt), h.map(|h| h.channel_hash),
                h.map(|h| h.next_hop), h.map(|h| h.relay_node),
                d.channel, d.portnum, d.port, d.data.as_ref().map(|v| v.to_string()), d.error, dup_of
            ],
        )?;
        let id = self.s.c.last_insert_rowid();
        if trusted {
            self.update_node(t, id, f, d)?;
        }
        Ok((id, dup_of))
    }

    fn update_node(&self, t: f64, frame_id: i64, f: &RxFrame, d: &Decoded) -> Result<()> {
        let h = d.header.unwrap();
        let hops = h.hops_taken();
        self.s.c.execute(
            "INSERT INTO nodes(num,id,first_heard,last_heard,last_frame_id,packets_heard,last_rssi,last_snr,last_hops_taken,last_relay_node,last_via_mqtt)
             VALUES(?1,printf('!%08x',?1),?2,?2,?3,1,?4,?5,?6,?7,?8)
             ON CONFLICT(num) DO UPDATE SET last_heard=excluded.last_heard,last_frame_id=excluded.last_frame_id,
               packets_heard=packets_heard+1,last_rssi=excluded.last_rssi,last_snr=excluded.last_snr,
               last_hops_taken=excluded.last_hops_taken,last_relay_node=excluded.last_relay_node,last_via_mqtt=excluded.last_via_mqtt",
            params![h.from, t, frame_id, f.rssi_dbm, f.snr_db, hops, h.relay_node, h.via_mqtt],
        )?;
        if hops == Some(0) && !h.via_mqtt {
            self.s.c.execute(
                "UPDATE nodes SET last_direct_heard=?2,last_direct_rssi=?3,last_direct_snr=?4 WHERE num=?1",
                params![h.from, t, f.rssi_dbm, f.snr_db],
            )?;
        }
        let (Some(port), Some(data)) = (d.portnum, &d.data) else { return Ok(()) };
        let p = &data["payload"];
        match port {
            4 => {
                self.s.c.execute(
                    "UPDATE nodes SET user_json=?2,user_time=?3,long_name=?4,short_name=?5,hw_model=?6,role=?7,public_key=?8,is_licensed=?9 WHERE num=?1",
                    params![
                        h.from, p.to_string(), t, p["long_name"].as_str(), p["short_name"].as_str(),
                        p["hw_model"].as_str(), p["role"].as_str().or(Some("CLIENT")), p["public_key"].as_str(),
                        p["is_licensed"].as_bool().unwrap_or(false)
                    ],
                )?;
            }
            3 if p.get("latitude_i").is_some() => {
                self.s.c.execute(
                    "UPDATE nodes SET position_json=?2,position_time=?3,latitude=?4,longitude=?5,altitude=?6 WHERE num=?1",
                    params![
                        h.from, p.to_string(), t,
                        p["latitude_i"].as_f64().map(|x| x * 1e-7), p["longitude_i"].as_f64().map(|x| x * 1e-7),
                        p["altitude"].as_i64()
                    ],
                )?;
            }
            67 if p.get("device_metrics").is_some() => {
                let m = &p["device_metrics"];
                self.s.c.execute(
                    "UPDATE nodes SET device_metrics_json=?2,device_metrics_time=?3,battery_level=?4,voltage=?5,channel_utilization=?6,air_util_tx=?7,uptime_seconds=?8 WHERE num=?1",
                    params![
                        h.from, m.to_string(), t, m["battery_level"].as_i64(), m["voltage"].as_f64(),
                        m["channel_utilization"].as_f64(), m["air_util_tx"].as_f64(), m["uptime_seconds"].as_i64()
                    ],
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_tx(
        &self, t: f64, raw: &[u8], airtime_ms: f64, h: &crate::packet::Header, encryption: &str, portnum: i32,
        data: &Value, attempt: u32, error: Option<&str>,
    ) -> Result<i64> {
        self.s.c.execute(
            "INSERT INTO tx_frames(session_id,tx_time,raw,len,airtime_ms,pkt_to,pkt_id,hop_limit,want_ack,channel_hash,encryption,portnum,port,data_json,attempt,error)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                self.session_id, t, raw, raw.len(), airtime_ms, h.to, h.id, h.hop_limit, h.want_ack, h.channel_hash,
                encryption, portnum, crate::proto::port_name(portnum), data.to_string(), attempt, error
            ],
        )?;
        Ok(self.s.c.last_insert_rowid())
    }
}
