//! Daemon: owns the radio, decodes and stores every frame, serves a local NDJSON API.
//!
//! API (TCP, one JSON object per line):
//! - `{"cmd":"status"}` -> one status object.
//! - `{"cmd":"subscribe"}` -> stream of frame events until the client disconnects.

use crate::config::Config;
use crate::decode::{decode_frame, to_json};
use crate::store::{self, Writer};
use anyhow::{Context, Result};
use meshtadpole_radio::{ch341, sx1262};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Shared {
    subscribers: Mutex<Vec<TcpStream>>,
    status: Mutex<Value>,
    frames: AtomicU64,
    stop: AtomicBool,
}

impl Shared {
    fn broadcast(&self, v: &Value) {
        let line = format!("{v}\n");
        let mut subs = self.subscribers.lock().unwrap();
        subs.retain_mut(|s| s.write_all(line.as_bytes()).is_ok());
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
                writeln!(w, "{s}")?;
            }
            Some("subscribe") => {
                writeln!(w, "{}", json!({"subscribed": true}))?;
                sh.subscribers.lock().unwrap().push(stream.try_clone()?);
            }
            other => writeln!(w, "{}", json!({"error": format!("unknown cmd {other:?}")}))?,
        }
    }
    Ok(())
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

    let w = Writer::start_session(store::open_rw(db)?, &usb_serial, &lora, &serde_json::to_string(cfg)?)?;
    let sh = Arc::new(Shared::default());
    *sh.status.lock().unwrap() = json!({
        "pid": std::process::id(),
        "session_id": w.session_id,
        "started": store::now(),
        "usb_serial": usb_serial,
        "db": db.display().to_string(),
        "freq_hz": lora.freq_hz, "bw_hz": lora.bw_hz, "sf": lora.sf, "cr": lora.cr,
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

    let res: Result<()> = (|| {
        while !sh.stop.load(Ordering::SeqCst) {
            match radio.poll(&lora)? {
                Some(sx1262::RxEvent::Frame(f)) => {
                    let t = store::now();
                    let airtime = lora.time_on_air_us(f.data.len()) / 1000.0;
                    let d = decode_frame(&f.data, f.crc_error, &channels);
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
                }
                Some(sx1262::RxEvent::HeaderError { rssi_inst_dbm, .. }) => {
                    let t = store::now();
                    let id = w.insert_header_error(t, rssi_inst_dbm)?;
                    hdr += 1;
                    sh.broadcast(&json!({"frame_id": id, "rx_time": t, "status": "header_error", "rssi": rssi_inst_dbm}));
                }
                None => {
                    if last_noise.elapsed() >= Duration::from_secs(1) {
                        let r = radio.rssi_inst()?;
                        n += 1;
                        nmin = nmin.min(r);
                        nmax = nmax.max(r);
                        nsum += r;
                        last_noise = Instant::now();
                    }
                    std::thread::sleep(Duration::from_millis(3));
                }
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

/// Connect to a running daemon.
pub fn connect(api: &str) -> Result<TcpStream> {
    TcpStream::connect(api).with_context(|| format!("daemon not reachable at {api}"))
}
