//! Daemon: owns the radio, decodes and stores every frame, transmits on request, serves a local NDJSON API.
//!
//! API (TCP, one JSON object per line):
//! - `{"cmd":"status"}` -> status object.
//! - `{"cmd":"subscribe"}` -> stream of frame events until disconnect.
//! - `{"cmd":"send", ...SendReq}` -> one result object after ack/response/timeout.

use crate::channel::Channel;
use crate::config::Config;
use crate::decode::{PkcCtx, decode_frame, to_json};
use crate::mesh::{self, SendReq};
use crate::store::{self, Writer};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use meshtadpole_radio::link::{self, Airtime, TxJob};
use meshtadpole_radio::{api, sx1262};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Per-transmission data handed through the radio link.
pub struct TxMeta {
    out: mesh::Outgoing,
    attempt: u32,
}

type Job = TxJob<TxMeta>;

struct Waiter {
    id: u32,
    /// Frame event plus decrypted `Data` bytes (daemon-internal; never sent to clients).
    tx: Sender<(Value, Option<Vec<u8>>)>,
}

struct Shared {
    cfg: Config,
    db: PathBuf,
    me: Option<u32>,
    channels: Vec<Channel>,
    pkc: Option<RwLock<PkcCtx>>,
    subscribers: api::Subscribers,
    waiters: Mutex<Vec<Waiter>>,
    tx_queue: Mutex<Sender<Job>>,
    airtime: Arc<Airtime>,
    passkeys: Mutex<HashMap<u32, (String, f64)>>,
    status: Mutex<Value>,
    frames: AtomicU64,
    stop: AtomicBool,
}

impl Shared {
    fn broadcast(&self, v: &Value) {
        self.subscribers.broadcast(&v.to_string());
    }

    fn notify_waiters(&self, ev: &Value, plain: Option<&Vec<u8>>) {
        let req = ev["data"]["request_id"].as_u64().map(|x| x as u32);
        let pid = ev["id"].as_u64().map(|x| x as u32);
        let from_me = self.me.is_some_and(|m| ev["from"] == json!(crate::packet::node_id(m)));
        self.waiters.lock().unwrap().retain(|w| {
            if req == Some(w.id) || (from_me && pid == Some(w.id)) {
                w.tx.send((ev.clone(), plain.cloned())).is_ok()
            } else {
                true
            }
        });
    }

}

fn handle_line(line: &str, w: &mut TcpStream, sh: &Shared) -> std::io::Result<()> {
    {
        let req: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                return writeln!(w, "{}", json!({"error": format!("bad request: {e}")}));
            }
        };
        match req["cmd"].as_str() {
            Some("status") => {
                let mut s = sh.status.lock().unwrap().clone();
                s["frames"] = json!(sh.frames.load(Ordering::Relaxed));
                s["tx_airtime_last_hour_ms"] = json!(sh.airtime.last_hour_ms());
                s["duty_cycle_limit"] = json!(sh.cfg.node.duty_cycle_limit);
                s["duty_cycle_enforce"] = json!(sh.cfg.node.duty_cycle_enforce);
                writeln!(w, "{s}")?;
            }
            Some("subscribe") => {
                writeln!(w, "{}", json!({"subscribed": true}))?;
                sh.subscribers.add(w.try_clone()?);
            }
            Some("send") => {
                let res = serde_json::from_value::<SendReq>(req.clone())
                    .map_err(|e| anyhow!("bad send request: {e}"))
                    .and_then(|r| do_send(&sh, &r));
                let out = match res {
                    Ok(v) => v,
                    Err(e) => json!({"error": format!("{e:#}")}),
                };
                if let Ok(c) = store::open_rw(&sh.db) {
                    let _ = c.execute(
                        "INSERT INTO requests(t,request_json,result_json) VALUES(?1,?2,?3)",
                        rusqlite::params![store::now(), req.to_string(), out.to_string()],
                    );
                }
                writeln!(w, "{out}")?;
            }
            Some("passkey") => {
                // Cached admin session passkey, if received < 140 s ago. Firmware reuses a key for 150 s after issue and it is valid 300 s after issue, so a received key is guaranteed valid only 150 s after receipt.
                let to = req["to"].as_u64().unwrap_or(0) as u32;
                let pk = sh.passkeys.lock().unwrap().get(&to).cloned();
                let v = match pk {
                    Some((k, t)) if store::now() - t < 140.0 => json!({"passkey": k, "age_s": store::now() - t}),
                    _ => json!({"passkey": null}),
                };
                writeln!(w, "{v}")?;
            }
            Some("admin_set") => {
                let out = match admin_set(&sh, &req) {
                    Ok(v) => v,
                    Err(e) => json!({"error": format!("{e:#}")}),
                };
                if let Ok(c) = store::open_rw(&sh.db) {
                    let _ = c.execute(
                        "INSERT INTO requests(t,request_json,result_json) VALUES(?1,?2,?3)",
                        rusqlite::params![store::now(), req.to_string(), out.to_string()],
                    );
                }
                writeln!(w, "{out}")?;
            }
            other => writeln!(w, "{}", json!({"error": format!("unknown cmd {other:?}")}))?,
        }
    }
    Ok(())
}

/// Transmit, retransmit if nothing heard, collect ack/responses/relay echoes.
fn do_send(sh: &Shared, req: &SendReq) -> Result<Value> {
    do_send_full(sh, req).map(|(v, _)| v)
}

/// As `do_send`, also returning decrypted `Data` bytes of each response (same order as `responses`).
fn do_send_full(sh: &Shared, req: &SendReq) -> Result<(Value, Vec<Vec<u8>>)> {
    let me = sh.me.ok_or_else(|| anyhow!("node.num not set in config"))?;
    let out = {
        let pk = sh.pkc.as_ref().map(|l| l.read().unwrap());
        mesh::build(me, sh.cfg.node.hop_limit, req, &sh.channels, pk.as_deref())?
    };
    let id = out.header.id;
    let (etx, erx) = channel::<(Value, Option<Vec<u8>>)>();
    let mut plains = Vec::new();
    sh.waiters.lock().unwrap().push(Waiter { id, tx: etx });
    let mut txs = Vec::new();
    let mut acks = Vec::new();
    let mut responses = Vec::new();
    let mut relays = Vec::new();
    let mut nak: Option<String> = None;
    let mut finished = false;
    for attempt in 0..=req.retries {
        let (dtx, drx) = channel();
        let delay = Duration::from_millis(rand::random::<u64>() % 400);
        sh.tx_queue.lock().unwrap().send(TxJob { frame: out.frame.clone(), meta: TxMeta { out: out.clone(), attempt }, delay, done: Some(dtx) })?;
        let tx = drx.recv_timeout(Duration::from_secs(60)).map_err(|_| anyhow!("tx queue timeout"))?.map_err(|e| anyhow!(e))?;
        txs.push(json!({"tx_frame_id": tx.id, "tx_time": tx.tx_time, "attempt": attempt, "len": tx.len, "airtime_ms": tx.airtime_ms}));
        let deadline = Instant::now() + Duration::from_secs_f64(req.wait_s);
        let mut heard_anything = false;
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            let Ok((ev, plain)) = erx.recv_timeout(left) else { break };
            heard_anything = true;
            if ev["from"] == json!(crate::packet::node_id(me)) && ev["id"].as_u64() == Some(id as u64) {
                relays.push(json!({"relay_node": ev["relay_node"], "hop_limit": ev["hop_limit"], "rssi": ev["rssi"], "snr": ev["snr"], "rx_time": ev["rx_time"], "frame_id": ev["frame_id"]}));
                continue;
            }
            if ev["dup_of"].is_number() {
                continue;
            }
            if ev["port"] == "ROUTING_APP" {
                let reason = ev["data"]["payload"]["error_reason"].as_str().unwrap_or("NONE").to_string();
                acks.push(ev.clone());
                if reason != "NONE" {
                    nak = Some(reason);
                    finished = true;
                    break;
                }
                if !req.want_response {
                    finished = true;
                    break;
                }
                continue;
            }
            if ev["port"] == "ADMIN_APP" {
                if let Some(pk) = ev["data"]["payload"]["session_passkey"].as_str() {
                    sh.passkeys.lock().unwrap().insert(out.header.to, (pk.to_string(), store::now()));
                }
            }
            responses.push(ev);
            plains.push(plain.unwrap_or_default());
            finished = true;
            break;
        }
        if finished || (heard_anything && !acks.is_empty()) {
            break;
        }
    }
    sh.waiters.lock().unwrap().retain(|w| w.id != id);
    Ok((json!({
        "packet_id": id,
        "to": crate::packet::node_id(out.header.to),
        "port": crate::proto::port_name(out.portnum),
        "encryption": out.encryption,
        "transmissions": txs,
        "acked": !acks.is_empty() && nak.is_none() || !responses.is_empty(),
        "nak": nak,
        "acks": acks,
        "responses": responses,
        "relays_heard": relays,
        "complete": finished,
    }), plains))
}

fn admin_req(to: u32, msg: &Value, want_response: bool, base: &Value) -> Result<SendReq> {
    Ok(SendReq {
        to,
        port: "ADMIN_APP".into(),
        payload_b64: base64::engine::general_purpose::STANDARD.encode(crate::proto::encode_json("AdminMessage", msg)?),
        want_ack: true,
        want_response,
        pkc: Some(true),
        channel: 0,
        hop_limit: base["hop_limit"].as_u64().map(|x| x as u8),
        request_id: None,
        wait_s: base["wait_s"].as_f64().unwrap_or(60.0),
        retries: base["retries"].as_u64().unwrap_or(1) as u32,
        from: None,
    })
}

/// Admin get; returns (send result, unredacted AdminMessage payload). Daemon-internal.
fn admin_get(sh: &Shared, to: u32, get: &Value, base: &Value) -> Result<(Value, Value)> {
    let (res, plains) = do_send_full(sh, &admin_req(to, get, true, base)?)?;
    if let Some(n) = res["nak"].as_str() {
        bail!("admin get NAK {n}");
    }
    let plain = plains.first().ok_or_else(|| anyhow!("admin get: no response from {}", crate::packet::node_id(to)))?;
    let (_, data) = crate::decode::data_json_opt(plain, false)?;
    Ok((res, data["payload"].clone()))
}

fn redacted_copy(v: &Value) -> Value {
    let mut c = v.clone();
    crate::decode::redact(&mut c);
    c
}

fn diff_json(d: &[(String, Value, Value)]) -> Value {
    let secret = |p: &str| crate::decode::is_redacted_field(p.rsplit('.').next().unwrap_or(p));
    json!(d
        .iter()
        .map(|(p, b, a)| if secret(p) {
            json!({"path": p, "before": crate::admin::REDACTED, "after": crate::admin::REDACTED})
        } else {
            json!({"path": p, "before": redacted_copy(b), "after": redacted_copy(a)})
        })
        .collect::<Vec<_>>())
}

/// Read-modify-write of one admin item. See docs/cli.md "Admin writes".
fn admin_set(sh: &Shared, req: &Value) -> Result<Value> {
    use crate::admin;
    let to = req["to"].as_u64().ok_or_else(|| anyhow!("to required"))? as u32;
    let get = &req["get"];
    let changes = &req["changes"];
    let apply = req["apply"].as_bool().unwrap_or(false);
    let allow_disruptive = req["allow_disruptive"].as_bool().unwrap_or(false);
    let verify_delay = req["verify_delay_s"].as_f64().unwrap_or(10.0);
    if !changes.is_object() {
        bail!("changes must be a JSON object");
    }
    if admin::contains_redacted(changes) {
        bail!("changes contain \"{}\"; secret values cannot be written through redacted output", admin::REDACTED);
    }
    let (_, resp) = admin_get(sh, to, get, req)?;
    let passkey = resp["session_passkey"].as_str().ok_or_else(|| anyhow!("response has no session_passkey"))?.to_string();
    let t = admin::target_from_response(&resp)?;
    let merged = admin::merge(&t.current, changes);
    let d = admin::diff(&t.current, &merged);
    let disruptive = admin::is_disruptive(&t);
    let mut out = json!({
        "node": crate::packet::node_id(to),
        "set_field": t.set_field, "variant": t.variant,
        "disruptive": disruptive,
        "before": redacted_copy(&t.current),
        "after": redacted_copy(&merged),
        "diff": diff_json(&d),
        "changed": !d.is_empty(),
        "applied": false,
    });
    if d.is_empty() {
        out["note"] = json!("no change; nothing sent");
        return Ok(out);
    }
    if t.variant.as_deref() == Some("security") {
        let ours = sh.pkc.as_ref().ok_or_else(|| anyhow!("no identity"))?.read().unwrap().id.public_b64();
        admin::check_security(&merged, &ours)?;
    }
    if disruptive && !allow_disruptive {
        out["refused"] = json!("disruptive change (security, lora, or channel 0) requires allow_disruptive");
        return Ok(out);
    }
    if !apply {
        out["dry_run"] = json!(true);
        return Ok(out);
    }
    let set = admin::set_message(&t, &merged, &passkey);
    // want_response: firmware returns admin errors (e.g. ADMIN_BAD_SESSION_KEY) as NAK and stays silent on
    // success. With want_ack only it ACKs on receipt and discards admin errors. Success is decided by read-back.
    let mut sreq = admin_req(to, &set, true, req)?;
    sreq.wait_s = req["set_wait_s"].as_f64().unwrap_or(20.0);
    sreq.retries = 0;
    let res = do_send(sh, &sreq)?;
    out["set_packet_id"] = res["packet_id"].clone();
    out["set_nak"] = res["nak"].clone();
    if res["nak"].is_string() {
        return Ok(out);
    }
    std::thread::sleep(Duration::from_secs_f64(verify_delay));
    match admin_get(sh, to, get, req) {
        Ok((_, rb)) => {
            let t2 = admin::target_from_response(&rb)?;
            let bad = admin::verify(&merged, &t2.current, changes);
            out["verified"] = json!(bad.is_empty());
            out["applied"] = json!(bad.is_empty());
            out["verify_mismatch"] = diff_json(&bad);
            if t.variant.as_deref() == Some("security") {
                out["identity_preserved"] = json!(
                    t2.current.get("public_key") == t.current.get("public_key")
                        && t2.current.get("private_key") == t.current.get("private_key")
                );
            }
        }
        Err(e) => {
            out["verified"] = Value::Null;
            out["verify_error"] = json!(format!("{e:#}"));
        }
    }
    Ok(out)
}

/// Firmware RoutingModule::getHopLimitForResponse.
fn response_hop_limit(h: &crate::packet::Header, ours: u8) -> u8 {
    if h.hop_start != 0 && h.hop_start >= h.hop_limit {
        let used = h.hop_start - h.hop_limit;
        if used > ours {
            return used;
        } else if used + 2 < ours {
            return used + 2;
        }
    }
    ours
}

/// ACK/NAK for want_ack packets to us (firmware ReliableRouter::sniffReceived) and minimal responders
/// (NodeInfo, traceroute) for requests to us. Replies go over the channel (firmware never uses PKC for
/// ROUTING/NODEINFO/TRACEROUTE). Per (from,id) at most once per 5 s.
fn auto_replies(
    sh: &Shared,
    d: &crate::decode::Decoded,
    f: &sx1262::RxFrame,
    recent: &mut HashMap<(u32, u32), Instant>,
) -> Vec<mesh::Outgoing> {
    let (Some(me), Some(h)) = (sh.me, d.header) else { return vec![] };
    if h.to != me || h.from == me || d.status == crate::decode::Status::CrcError {
        return vec![];
    }
    recent.retain(|_, t| t.elapsed() < Duration::from_secs(5));
    if recent.contains_key(&(h.from, h.id)) {
        return vec![];
    }
    let ch_idx = d
        .channel
        .as_deref()
        .and_then(|n| sh.channels.iter().position(|c| c.name == n))
        .unwrap_or(0);
    let resp_hops = response_hop_limit(&h, sh.cfg.node.hop_limit);
    let mk = |port: &str, payload: Vec<u8>, request_id: u32, hop_limit: u8, want_ack: bool| -> Option<mesh::Outgoing> {
        let req = SendReq {
            to: h.from,
            port: port.into(),
            payload_b64: base64::engine::general_purpose::STANDARD.encode(payload),
            want_ack,
            want_response: false,
            pkc: Some(false),
            channel: ch_idx,
            hop_limit: Some(hop_limit),
            request_id: Some(request_id),
            wait_s: 0.0,
            retries: 0,
            from: None,
        };
        mesh::build(me, sh.cfg.node.hop_limit, &req, &sh.channels, None).ok()
    };
    let routing = |reason: &str| crate::proto::encode_json("Routing", &json!({"error_reason": reason})).unwrap_or_default();
    let mut out = Vec::new();
    let data = d.data.as_ref();
    let has_req_or_reply = data.is_some_and(|x| x.get("request_id").is_some() || x.get("reply_id").is_some());
    let want_response = data.is_some_and(|x| x["want_response"].as_bool().unwrap_or(false));

    // Responders (a response replaces the ACK, as in firmware).
    let mut responded = false;
    if d.status == crate::decode::Status::Ok && want_response && !has_req_or_reply && d.portnum != Some(5) {
        match d.portnum {
            Some(4) => {
                let pubk = sh.pkc.as_ref().map(|p| p.read().unwrap().id.public_b64());
                if let Ok(p) = mesh::user_payload(&sh.cfg, me, pubk) {
                    out.extend(mk("NODEINFO_APP", p, h.id, resp_hops, false));
                    responded = true;
                }
            }
            Some(70) => {
                let mut rd = data.map(|x| x["payload"].clone()).filter(|v| v.is_object()).unwrap_or(json!({}));
                let snr = (f.snr_db * 4.0).round().clamp(-127.0, 127.0) as i64;
                let mut s = rd["snr_towards"].as_array().cloned().unwrap_or_default();
                s.push(json!(snr));
                rd["snr_towards"] = json!(s);
                if let Ok(p) = crate::proto::encode_json("RouteDiscovery", &rd) {
                    out.extend(mk("TRACEROUTE_APP", p, h.id, resp_hops, false));
                    responded = true;
                }
            }
            // Unhandled request: firmware MeshModule sends NO_RESPONSE when no module replies;
            // AdminModule answers unauthorized admin with NOT_AUTHORIZED.
            Some(6) => {
                out.extend(mk("ROUTING_APP", routing("NOT_AUTHORIZED"), h.id, resp_hops, false));
                responded = true;
            }
            Some(_) => {
                out.extend(mk("ROUTING_APP", routing("NO_RESPONSE"), h.id, resp_hops, false));
                responded = true;
            }
            None => {}
        }
    }
    if h.want_ack && !responded {
        match d.status {
            crate::decode::Status::Ok => {
                if !has_req_or_reply {
                    out.extend(mk("ROUTING_APP", routing("NONE"), h.id, resp_hops, false));
                } else if h.hops_taken() == Some(0) || h.next_hop != 0 {
                    out.extend(mk("ROUTING_APP", routing("NONE"), h.id, 0, false));
                }
            }
            crate::decode::Status::Pkc => out.extend(mk("ROUTING_APP", routing("PKI_UNKNOWN_PUBKEY"), h.id, resp_hops, false)),
            crate::decode::Status::NoChannel | crate::decode::Status::DecodeError => {
                out.extend(mk("ROUTING_APP", routing("NO_CHANNEL"), h.id, resp_hops, false))
            }
            _ => {}
        }
    }
    if !out.is_empty() {
        recent.insert((h.from, h.id), Instant::now());
    }
    out
}

fn load_keys(db: &Path) -> HashMap<u32, [u8; 32]> {
    let mut m = HashMap::new();
    let Ok(c) = store::open_rw(db) else { return m };
    let Ok(mut st) = c.prepare("SELECT num, public_key FROM nodes WHERE public_key IS NOT NULL") else { return m };
    let rows = st.query_map([], |r| Ok((r.get::<_, u32>(0)?, r.get::<_, String>(1)?)));
    if let Ok(rows) = rows {
        for (n, k) in rows.flatten() {
            if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(k) {
                if let Ok(a) = <[u8; 32]>::try_from(b.as_slice()) {
                    m.insert(n, a);
                }
            }
        }
    }
    m
}

/// Protocol side of the radio link.
struct Mt {
    sh: Arc<Shared>,
    w: Writer,
    acked_recent: HashMap<(u32, u32), Instant>,
}

impl link::Handler for Mt {
    type Meta = TxMeta;

    fn event(&mut self, ev: &link::Event) -> Result<(), link::BoxError> {
        self.w.s.radio_event(ev.name(), ev.detail())?;
        let mut st = self.sh.status.lock().unwrap();
        match ev {
            link::Event::Connected { usb_serial } => {
                st["radio"] = json!("connected");
                st["usb_serial"] = json!(usb_serial);
                eprintln!("{}", json!({"event": "connected", "usb_serial": usb_serial}));
            }
            link::Event::Disconnected { error } => {
                st["radio"] = json!("disconnected");
                eprintln!("{}", json!({"event": "disconnected", "error": error}));
            }
            link::Event::OpenFailed { error } => eprintln!("{}", json!({"event": "open_failed", "error": error})),
            link::Event::RxRestored { detail } => eprintln!("{}", json!({"event": "rx_restored", "detail": detail})),
        }
        Ok(())
    }

    fn frame(&mut self, t: f64, f: &sx1262::RxFrame, airtime: f64) -> Result<Vec<Job>, link::BoxError> {
        let sh = &self.sh;
        let d = {
            let pk = sh.pkc.as_ref().map(|l| l.read().unwrap());
            decode_frame(&f.data, f.crc_error, &sh.channels, pk.as_deref())
        };
        if d.portnum == Some(4) {
            if let (Some(pk), Some(h), Some(k)) = (&sh.pkc, d.header, d.data.as_ref().and_then(|x| x["payload"]["public_key"].as_str())) {
                if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(k) {
                    if let Ok(a) = <[u8; 32]>::try_from(b.as_slice()) {
                        pk.write().unwrap().keys.insert(h.from, a);
                    }
                }
            }
        }
        let (id, dup_of) = self.w.insert_frame(t, f, airtime, &d)?;
        sh.frames.fetch_add(1, Ordering::Relaxed);
        let mut ev = to_json(&d);
        ev["frame_id"] = json!(id);
        ev["rx_time"] = json!(t);
        ev["rssi"] = json!(f.rssi_dbm);
        ev["snr"] = json!(f.snr_db);
        ev["freq_error_hz"] = json!(f.freq_error_hz.round());
        ev["len"] = json!(f.data.len());
        ev["airtime_ms"] = json!((airtime * 10.0).round() / 10.0);
        ev["dup_of"] = json!(dup_of);
        sh.broadcast(&ev);
        sh.notify_waiters(&ev, d.plaintext.as_ref());
        Ok(auto_replies(sh, &d, f, &mut self.acked_recent)
            .into_iter()
            .map(|out| TxJob {
                frame: out.frame.clone(),
                meta: TxMeta { out, attempt: 0 },
                delay: Duration::from_millis(100 + rand::random::<u64>() % 300),
                done: None,
            })
            .collect())
    }

    fn header_error(&mut self, t: f64, rssi: f32) -> Result<(), link::BoxError> {
        let id = self.w.insert_header_error(t, rssi)?;
        self.sh.broadcast(&json!({"frame_id": id, "rx_time": t, "status": "header_error", "rssi": rssi}));
        Ok(())
    }

    fn transmitted(&mut self, job: &Job, t: f64, airtime: f64, error: Option<&str>) -> Result<i64, link::BoxError> {
        let o = &job.meta.out;
        Ok(self.w.insert_tx(t, &o.frame, airtime, &o.header, o.encryption, o.portnum, &o.data_json, job.meta.attempt, error)?)
    }

    fn stats(&mut self, s: &link::Stats) -> Result<(), link::BoxError> {
        Ok(self.w.s.radio_stats(s)?)
    }

    fn heartbeat(&mut self) -> Result<(), link::BoxError> {
        Ok(self.w.s.heartbeat()?)
    }
}

pub fn run(cfg: &Config, db: &Path) -> Result<()> {
    let listener = TcpListener::bind(&cfg.api).with_context(|| format!("bind API {} (daemon already running?)", cfg.api))?;
    let channels = cfg.channels()?;
    let lora = cfg.lora()?;
    let w = Writer::start_session(store::open_rw(db)?, "", &lora, &serde_json::to_string(&redacted(cfg))?)?;
    let me = cfg.node.num;
    let pkc = match (&cfg.node.private_key, me) {
        (Some(k), Some(me)) => Some(RwLock::new(PkcCtx { me, id: crate::pkc::Identity::from_b64(k)?, keys: load_keys(db) })),
        _ => None,
    };
    let (qtx, qrx) = channel::<Job>();
    let airtime = Arc::new(Airtime::default());
    let sh = Arc::new(Shared {
        cfg: cfg.clone(),
        db: db.to_path_buf(),
        me,
        channels,
        pkc,
        subscribers: api::Subscribers::default(),
        waiters: Mutex::new(Vec::new()),
        tx_queue: Mutex::new(qtx),
        airtime: airtime.clone(),
        passkeys: Mutex::new(HashMap::new()),
        status: Mutex::new(Value::Null),
        frames: AtomicU64::new(0),
        stop: AtomicBool::new(false),
    });
    *sh.status.lock().unwrap() = json!({
        "pid": std::process::id(),
        "session_id": w.session_id,
        "started": store::now(),
        "radio": "disconnected",
        "usb_serial": null,
        "db": db.display().to_string(),
        "node_num": me.map(crate::packet::node_id),
        "public_key": sh.pkc.as_ref().map(|p| p.read().unwrap().id.public_b64()),
        "freq_hz": lora.freq_hz, "bw_hz": lora.bw_hz, "sf": lora.sf, "cr": lora.cr, "tx_power_dbm": lora.tx_power_dbm,
        "channels": cfg.channels.iter().map(|c| &c.name).collect::<Vec<_>>(),
    });
    {
        let sh = sh.clone();
        std::thread::spawn(move || api::serve(listener, move |line: &str, w: &mut TcpStream| handle_line(line, w, &sh)));
    }
    {
        let sh = sh.clone();
        ctrlc::set_handler(move || sh.stop.store(true, Ordering::SeqCst)).ok();
    }
    eprintln!("{}", json!({"event": "started", "status": *sh.status.lock().unwrap()}));
    let lc = link::LinkConfig {
        lora,
        usb_serial: cfg.usb_serial.clone(),
        duty_cycle_limit: cfg.node.duty_cycle_limit,
        duty_cycle_enforce: cfg.node.duty_cycle_enforce,
        stats_interval: Duration::from_secs(60),
        agc_reset_interval: cfg.radio.agc_reset_interval_s.filter(|&s| s > 0).map(Duration::from_secs),
    };
    let mut h = Mt { sh: sh.clone(), w, acked_recent: HashMap::new() };
    let res = link::run(&lc, &mut h, &qrx, &airtime, &sh.stop).map_err(|e| anyhow!(e));
    h.w.s.end()?;
    eprintln!("{}", json!({"event": "stopped", "error": res.as_ref().err().map(|e| format!("{e:#}"))}));
    res
}

/// Config with private key removed, for storage.
pub fn redacted(cfg: &Config) -> Config {
    let mut c = cfg.clone();
    if c.node.private_key.is_some() {
        c.node.private_key = Some("<redacted>".into());
    }
    c
}

/// Connect to a running daemon.
pub fn connect(api: &str) -> Result<TcpStream> {
    TcpStream::connect(api).with_context(|| format!("daemon not reachable at {api}"))
}

/// One request/response round trip with the daemon.
pub fn call(api: &str, req: &Value) -> Result<Value> {
    let mut s = connect(api)?;
    writeln!(s, "{req}")?;
    let mut line = String::new();
    BufReader::new(s).read_line(&mut line)?;
    let v: Value = serde_json::from_str(&line)?;
    if let Some(e) = v.get("error") {
        bail!("{}", e.as_str().unwrap_or(&e.to_string()));
    }
    Ok(v)
}
