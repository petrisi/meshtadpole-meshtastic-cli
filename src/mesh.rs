//! Building packets we originate.

use crate::channel::{Channel, ctr_apply};
use crate::decode::PkcCtx;
use crate::packet::{BROADCAST, Header};
use crate::{pkc, proto};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendReq {
    pub to: u32,
    /// PortNum name, e.g. TEXT_MESSAGE_APP.
    pub port: String,
    /// Encoded payload bytes, base64.
    pub payload_b64: String,
    #[serde(default)]
    pub want_ack: bool,
    #[serde(default)]
    pub want_response: bool,
    /// None = auto (PKC for unicast when keys available).
    #[serde(default)]
    pub pkc: Option<bool>,
    /// Channel index for channel encryption.
    #[serde(default)]
    pub channel: usize,
    #[serde(default)]
    pub hop_limit: Option<u8>,
    #[serde(default)]
    pub request_id: Option<u32>,
    /// Seconds to wait for ack/response per attempt.
    #[serde(default = "default_wait")]
    pub wait_s: f64,
    /// Retransmissions after the first attempt if nothing is heard.
    #[serde(default)]
    pub retries: u32,
}

fn default_wait() -> f64 {
    60.0
}

#[derive(Debug, Clone)]
pub struct Outgoing {
    pub header: Header,
    pub frame: Vec<u8>,
    pub encryption: &'static str,
    pub data_json: Value,
    pub portnum: i32,
}

pub fn port_number(name: &str) -> Result<i32> {
    let e = proto::pool().get_enum_by_name("meshtastic.PortNum").unwrap();
    if let Ok(n) = name.parse::<i32>() {
        return Ok(n);
    }
    e.get_value_by_name(&name.to_uppercase())
        .map(|v| v.number())
        .ok_or_else(|| anyhow!("unknown port {name}"))
}

pub fn random_id() -> u32 {
    loop {
        let x: u32 = rand::random();
        if x != 0 {
            return x;
        }
    }
}

pub fn build(me: u32, hop_limit_default: u8, req: &SendReq, channels: &[Channel], pkc_ctx: Option<&PkcCtx>) -> Result<Outgoing> {
    let portnum = port_number(&req.port)?;
    let payload = base64::engine::general_purpose::STANDARD.decode(&req.payload_b64).context("payload_b64")?;
    let mut data = json!({
        "portnum": proto::port_name(portnum).unwrap_or_else(|| portnum.to_string()),
        "payload": req.payload_b64,
    });
    if req.want_response {
        data["want_response"] = json!(true);
    }
    if let Some(r) = req.request_id {
        data["request_id"] = json!(r);
    }
    let plain = proto::encode_json("Data", &data)?;
    let _ = payload;
    let id = random_id();
    let hop_limit = req.hop_limit.unwrap_or(hop_limit_default).min(7);
    let unicast = req.to != BROADCAST;
    let peer_key = pkc_ctx.and_then(|c| c.keys.get(&req.to));
    let use_pkc = match req.pkc {
        Some(true) => {
            if !unicast {
                bail!("PKC requires a unicast destination");
            }
            if pkc_ctx.is_none() {
                bail!("PKC requires node.private_key in config");
            }
            if peer_key.is_none() {
                bail!("no public key known for {}", crate::packet::node_id(req.to));
            }
            true
        }
        Some(false) => false,
        None => unicast && pkc_ctx.is_some() && peer_key.is_some(),
    };
    let (hash, body, enc) = if use_pkc {
        let ctx = pkc_ctx.unwrap();
        let shared = ctx.id.shared(peer_key.unwrap());
        (0u8, pkc::encrypt(&shared, me, id, &plain, random_id())?, "pkc")
    } else {
        let ch = channels.get(req.channel).ok_or_else(|| anyhow!("no channel index {}", req.channel))?;
        let mut b = plain.clone();
        ctr_apply(&ch.key, me, id, &mut b)?;
        (ch.hash, b, "channel")
    };
    let header = Header {
        to: req.to,
        from: me,
        id,
        hop_limit,
        want_ack: req.want_ack,
        via_mqtt: false,
        hop_start: hop_limit,
        channel_hash: hash,
        next_hop: 0,
        relay_node: (me & 0xFF) as u8,
    };
    let mut frame = header.encode().to_vec();
    frame.extend_from_slice(&body);
    if frame.len() > 255 {
        bail!("frame too long: {} bytes", frame.len());
    }
    Ok(Outgoing { header, frame, encryption: enc, data_json: crate::decode::data_json(&plain)?.1, portnum })
}

/// NodeInfo `User` payload for our identity.
pub fn user_payload(cfg: &crate::config::Config, me: u32, public_b64: Option<String>) -> Result<Vec<u8>> {
    let mut u = json!({
        "id": crate::packet::node_id(me),
        "long_name": cfg.node.long_name,
        "short_name": cfg.node.short_name,
        "hw_model": cfg.node.hw_model,
        "role": cfg.node.role,
    });
    if let Some(p) = public_b64 {
        u["public_key"] = json!(p);
    }
    proto::encode_json("User", &u)
}
