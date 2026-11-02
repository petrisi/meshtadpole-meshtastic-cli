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
use meshtadpole_radio::{ch341, sx1262};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

struct TxJob {
    out: mesh::Outgoing,
    attempt: u32,
    done: Sender<Result<Value>>,
}

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
    subscribers: Mutex<Vec<TcpStream>>,
    waiters: Mutex<Vec<Waiter>>,
    tx_queue: Mutex<Sender<TxJob>>,
    tx_log: Mutex<VecDeque<(Instant, f64)>>,
    passkeys: Mutex<HashMap<u32, (String, f64)>>,
    status: Mutex<Value>,
    frames: AtomicU64,
    stop: AtomicBool,
}

impl Shared {
    fn broadcast(&self, v: &Value) {
        let line = format!("{v}\n");
        self.subscribers.lock().unwrap().retain_mut(|s| s.write_all(line.as_bytes()).is_ok());
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

    fn airtime_last_hour_ms(&self) -> f64 {
        let mut log = self.tx_log.lock().unwrap();
        while log.front().is_some_and(|(t, _)| t.elapsed() > Duration::from_secs(3600)) {
            log.pop_front();
        }
        log.iter().map(|(_, a)| a).sum()
    }
}

fn serve(listener: TcpListener, sh: Arc<Shared>) {
    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let sh = sh.clone();
        std::thread::spawn(move || {
            let _ = handle_client(stream, sh);
        });
    }
}

fn handle_client(stream: TcpStream, sh: Arc<Shared>) -> Result<()> {
    let mut w = stream.try_clone()?;
    let r = BufReader::new(stream.try_clone()?);
    for line in r.lines() {
        let line = line?;
        let req: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                writeln!(w, "{}", json!({"error": format!("bad request: {e}")}))?;
                continue;
            }
        };
        match req["cmd"].as_str() {
            Some("status") => {
                let mut s = sh.status.lock().unwrap().clone();
                s["frames"] = json!(sh.frames.load(Ordering::Relaxed));
                s["tx_airtime_last_hour_ms"] = json!(sh.airtime_last_hour_ms());
                s["duty_cycle_limit"] = json!(sh.cfg.node.duty_cycle_limit);
                s["duty_cycle_enforce"] = json!(sh.cfg.node.duty_cycle_enforce);
                writeln!(w, "{s}")?;
            }
            Some("subscribe") => {
                writeln!(w, "{}", json!({"subscribed": true}))?;
                sh.subscribers.lock().unwrap().push(stream.try_clone()?);
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
        sh.tx_queue.lock().unwrap().send(TxJob { out: out.clone(), attempt, done: dtx })?;
        let tx = drx.recv_timeout(Duration::from_secs(60)).map_err(|_| anyhow!("tx queue timeout"))??;
        txs.push(tx);
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
    let res = do_send(sh, &admin_req(to, &set, false, req)?)?;
    out["set_packet_id"] = res["packet_id"].clone();
    out["set_transmissions"] = json!(res["transmissions"].as_array().map(|a| a.len()));
    out["set_nak"] = res["nak"].clone();
    out["set_acked"] = res["acked"].clone();
    if res["nak"].is_string() {
        return Ok(out);
    }
    out["applied"] = json!(res["acked"].as_bool().unwrap_or(false));
    std::thread::sleep(Duration::from_secs_f64(verify_delay));
    match admin_get(sh, to, get, req) {
        Ok((_, rb)) => {
            let t2 = admin::target_from_response(&rb)?;
            let bad = admin::verify(&merged, &t2.current, changes);
            out["verified"] = json!(bad.is_empty());
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

/// Channel busy: preamble/header detected without completion. Stale flags (> 3 s) are cleared.
fn channel_busy(radio: &mut sx1262::Sx1262, since: &mut Option<Instant>) -> Result<bool> {
    let irq = radio.get_irq_status()?;
    let active = irq & (sx1262::irq::PREAMBLE_DETECTED | sx1262::irq::HEADER_VALID) != 0 && irq & sx1262::irq::RX_DONE == 0;
    if !active {
        *since = None;
        return Ok(false);
    }
    let t = *since.get_or_insert_with(Instant::now);
    if t.elapsed() > Duration::from_secs(3) {
        radio.clear_irq(sx1262::irq::PREAMBLE_DETECTED | sx1262::irq::HEADER_VALID)?;
        *since = None;
        return Ok(false);
    }
    Ok(true)
}

pub fn run(cfg: &Config, db: &Path) -> Result<()> {
    let listener = TcpListener::bind(&cfg.api).with_context(|| format!("bind API {} (daemon already running?)", cfg.api))?;
    let channels = cfg.channels()?;
    let lora = cfg.lora()?;
    let bus = ch341::Ch341::open(cfg.usb_serial.as_deref()).context("open MeshTadpole")?;
    let usb_serial = bus.info.serial.clone().unwrap_or_default();
    let mut radio = sx1262::Sx1262::new(bus, sx1262::MESHTADPOLE_PINS)?;
    radio.configure(&lora).context("configure SX1262")?;
    radio.start_rx()?;

    let w = Writer::start_session(store::open_rw(db)?, &usb_serial, &lora, &serde_json::to_string(&redacted(cfg))?)?;
    let me = cfg.node.num;
    let pkc = match (&cfg.node.private_key, me) {
        (Some(k), Some(me)) => Some(RwLock::new(PkcCtx { me, id: crate::pkc::Identity::from_b64(k)?, keys: load_keys(db) })),
        _ => None,
    };
    let (qtx, qrx): (Sender<TxJob>, Receiver<TxJob>) = channel();
    let sh = Arc::new(Shared {
        cfg: cfg.clone(),
        db: db.to_path_buf(),
        me,
        channels: channels.clone(),
        pkc,
        subscribers: Mutex::new(Vec::new()),
        waiters: Mutex::new(Vec::new()),
        tx_queue: Mutex::new(qtx),
        tx_log: Mutex::new(VecDeque::new()),
        passkeys: Mutex::new(HashMap::new()),
        status: Mutex::new(Value::Null),
        frames: AtomicU64::new(0),
        stop: AtomicBool::new(false),
    });
    *sh.status.lock().unwrap() = json!({
        "pid": std::process::id(),
        "session_id": w.session_id,
        "started": store::now(),
        "usb_serial": usb_serial,
        "db": db.display().to_string(),
        "node_num": me.map(crate::packet::node_id),
        "public_key": sh.pkc.as_ref().map(|p| p.read().unwrap().id.public_b64()),
        "freq_hz": lora.freq_hz, "bw_hz": lora.bw_hz, "sf": lora.sf, "cr": lora.cr, "tx_power_dbm": lora.tx_power_dbm,
        "channels": cfg.channels.iter().map(|c| &c.name).collect::<Vec<_>>(),
    });
    {
        let sh = sh.clone();
        std::thread::spawn(move || serve(listener, sh));
    }
    {
        let sh = sh.clone();
        ctrlc::set_handler(move || sh.stop.store(true, Ordering::SeqCst)).ok();
    }
    eprintln!("{}", json!({"event": "started", "status": *sh.status.lock().unwrap()}));

    let mut stat_t0 = store::now();
    let mut last_stat = Instant::now();
    let mut last_noise = Instant::now();
    let (mut n, mut nmin, mut nsum, mut nmax) = (0u32, f32::MAX, 0f32, f32::MIN);
    let (mut frames, mut crc, mut hdr, mut air) = (0u32, 0u32, 0u32, 0f64);
    let mut pending: VecDeque<(TxJob, Instant)> = VecDeque::new();
    let mut busy_since: Option<Instant> = None;

    let res: Result<()> = (|| {
        while !sh.stop.load(Ordering::SeqCst) {
            while let Ok(job) = qrx.try_recv() {
                let backoff = Duration::from_millis(rand::random::<u64>() % 400);
                pending.push_back((job, Instant::now() + backoff));
            }
            let mut did_rx = false;
            match radio.poll(&lora)? {
                Some(sx1262::RxEvent::Frame(f)) => {
                    did_rx = true;
                    let t = store::now();
                    let airtime = lora.time_on_air_us(f.data.len()) / 1000.0;
                    let d = {
                        let pk = sh.pkc.as_ref().map(|l| l.read().unwrap());
                        decode_frame(&f.data, f.crc_error, &channels, pk.as_deref())
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
                    let (id, dup_of) = w.insert_frame(t, &f, airtime, &d)?;
                    frames += 1;
                    crc += f.crc_error as u32;
                    air += airtime;
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
                }
                Some(sx1262::RxEvent::HeaderError { rssi_inst_dbm, .. }) => {
                    did_rx = true;
                    let t = store::now();
                    let id = w.insert_header_error(t, rssi_inst_dbm)?;
                    hdr += 1;
                    sh.broadcast(&json!({"frame_id": id, "rx_time": t, "status": "header_error", "rssi": rssi_inst_dbm}));
                }
                None => {}
            }
            if !did_rx {
                if let Some((_, at)) = pending.front() {
                    if Instant::now() >= *at {
                        if channel_busy(&mut radio, &mut busy_since)? {
                            let (job, _) = pending.pop_front().unwrap();
                            let at = Instant::now() + Duration::from_millis(200 + rand::random::<u64>() % 600);
                            pending.push_front((job, at));
                        } else {
                            let (job, _) = pending.pop_front().unwrap();
                            let r = transmit(&mut radio, &lora, &sh, &w, &job);
                            let _ = job.done.send(r);
                        }
                    }
                } else if last_noise.elapsed() >= Duration::from_secs(1) {
                    let r = radio.rssi_inst()?;
                    n += 1;
                    nmin = nmin.min(r);
                    nmax = nmax.max(r);
                    nsum += r;
                    last_noise = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(3));
            }
            if last_stat.elapsed() >= Duration::from_secs(60) {
                let t1 = store::now();
                let avg = if n > 0 { nsum / n as f32 } else { f32::NAN };
                w.insert_radio_stats(stat_t0, t1, n, nmin, avg, nmax, frames, crc, hdr, air)?;
                w.heartbeat()?;
                (n, nmin, nsum, nmax) = (0, f32::MAX, 0.0, f32::MIN);
                (frames, crc, hdr, air) = (0, 0, 0, 0.0);
                stat_t0 = t1;
                last_stat = Instant::now();
            }
        }
        Ok(())
    })();
    w.end_session()?;
    eprintln!("{}", json!({"event": "stopped", "error": res.as_ref().err().map(|e| format!("{e:#}"))}));
    res
}

fn transmit(radio: &mut sx1262::Sx1262, lora: &sx1262::LoraConfig, sh: &Shared, w: &Writer, job: &TxJob) -> Result<Value> {
    let o = &job.out;
    let airtime = lora.time_on_air_us(o.frame.len()) / 1000.0;
    let budget = sh.cfg.node.duty_cycle_limit * 3_600_000.0;
    let used = sh.airtime_last_hour_ms();
    if sh.cfg.node.duty_cycle_enforce && used + airtime > budget {
        bail!("duty cycle limit: {used:.0} ms used of {budget:.0} ms in last hour");
    }
    let t = store::now();
    let r = radio.transmit(lora, &o.frame);
    let err = r.as_ref().err().map(|e| e.to_string());
    let fid = w.insert_tx(t, &o.frame, airtime, &o.header, o.encryption, o.portnum, &o.data_json, job.attempt, err.as_deref())?;
    r?;
    sh.tx_log.lock().unwrap().push_back((Instant::now(), airtime));
    Ok(json!({"tx_frame_id": fid, "tx_time": t, "attempt": job.attempt, "len": o.frame.len(), "airtime_ms": (airtime * 10.0).round() / 10.0}))
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
