//! Frame -> header + decrypted/decoded payload.

use crate::channel::{Channel, ctr_apply};
use crate::packet::{HEADER_LEN, Header};
use crate::proto::{self, PayloadKind};
use base64::Engine;
use serde::Serialize;
use serde_json::{Value, json};

/// Decode outcome. Stored verbatim in `rx_frames.status`.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Decrypted and parsed as `meshtastic.Data`.
    Ok,
    /// Radio reported CRC error; header/payload untrusted.
    CrcError,
    /// Radio reported LoRa header error; no payload.
    HeaderError,
    /// Shorter than the 16-byte header.
    Short,
    /// No configured channel matches the channel hash (or it is a PKC packet not addressed to us).
    NoChannel,
    /// Channel hash 0 and unicast: PKC (x25519/AES-CCM) packet; no key to decrypt.
    Pkc,
    /// Channel hash matched but decryption did not yield a valid `Data`.
    DecodeError,
}

#[derive(Debug, Clone, Serialize)]
pub struct Decoded {
    pub status: Status,
    pub header: Option<Header>,
    pub channel: Option<String>,
    pub portnum: Option<i32>,
    pub port: Option<String>,
    /// `meshtastic.Data` as JSON with `payload` replaced by the decoded payload.
    pub data: Option<Value>,
    /// Plaintext Data bytes (after decryption).
    #[serde(skip)]
    pub plaintext: Option<Vec<u8>>,
    pub error: Option<String>,
}

impl Decoded {
    fn bare(status: Status, header: Option<Header>) -> Decoded {
        Decoded { status, header, channel: None, portnum: None, port: None, data: None, plaintext: None, error: None }
    }
}

/// Decode `meshtastic.Data` bytes into JSON with the payload expanded per portnum.
pub fn data_json(plain: &[u8]) -> anyhow::Result<(i32, Value)> {
    let mut v = proto::decode_json("Data", plain)?;
    let portnum = crate::proto::pool()
        .get_message_by_name("meshtastic.Data")
        .and_then(|d| prost_reflect::DynamicMessage::decode(d, plain).ok())
        .and_then(|m| m.get_field_by_name("portnum").and_then(|f| f.as_enum_number()))
        .unwrap_or(0);
    if portnum == 0 {
        anyhow::bail!("portnum UNKNOWN_APP");
    }
    let payload_b64 = v.get("payload").and_then(|p| p.as_str()).unwrap_or("").to_string();
    let payload = base64::engine::general_purpose::STANDARD.decode(&payload_b64).unwrap_or_default();
    let obj = v.as_object_mut().unwrap();
    match proto::payload_kind(portnum) {
        PayloadKind::Text => {
            obj.insert("payload".into(), Value::String(String::from_utf8_lossy(&payload).into_owned()));
        }
        PayloadKind::Message(m) => match proto::decode_json(m, &payload) {
            Ok(p) => {
                obj.insert("payload".into(), p);
                obj.insert("payload_type".into(), Value::String(m.into()));
            }
            Err(e) => {
                obj.insert("payload".into(), Value::String(hex::encode(&payload)));
                obj.insert("payload_error".into(), Value::String(format!("{e:#}")));
            }
        },
        PayloadKind::Raw => {
            obj.insert("payload".into(), Value::String(hex::encode(&payload)));
            obj.insert("payload_type".into(), Value::String("hex".into()));
        }
    }
    Ok((portnum, v))
}

pub fn decode_frame(raw: &[u8], crc_error: bool, channels: &[Channel]) -> Decoded {
    if crc_error {
        return Decoded::bare(Status::CrcError, Header::parse(raw));
    }
    let Some(h) = Header::parse(raw) else {
        return Decoded::bare(Status::Short, None);
    };
    let enc = &raw[HEADER_LEN..];
    let mut last_err = None;
    let mut matched = false;
    for ch in channels.iter().filter(|c| c.hash == h.channel_hash) {
        matched = true;
        let mut buf = enc.to_vec();
        if let Err(e) = ctr_apply(&ch.key, h.from, h.id, &mut buf) {
            last_err = Some(format!("{e:#}"));
            continue;
        }
        match data_json(&buf) {
            Ok((portnum, data)) => {
                return Decoded {
                    status: Status::Ok,
                    header: Some(h),
                    channel: Some(ch.name.clone()),
                    portnum: Some(portnum),
                    port: proto::port_name(portnum),
                    data: Some(data),
                    plaintext: Some(buf),
                    error: None,
                };
            }
            Err(e) => last_err = Some(format!("{e:#}")),
        }
    }
    if matched {
        let mut d = Decoded::bare(Status::DecodeError, Some(h));
        d.error = last_err;
        return d;
    }
    if h.channel_hash == 0 && h.to != crate::packet::BROADCAST && enc.len() > 12 {
        return Decoded::bare(Status::Pkc, Some(h));
    }
    Decoded::bare(Status::NoChannel, Some(h))
}

/// Compact JSON for a decoded frame, used by `listen` and `decode`.
pub fn to_json(d: &Decoded) -> Value {
    let mut v = json!({ "status": d.status });
    let o = v.as_object_mut().unwrap();
    if let Some(h) = &d.header {
        o.insert("from".into(), json!(crate::packet::node_id(h.from)));
        o.insert("to".into(), json!(crate::packet::node_id(h.to)));
        o.insert("id".into(), json!(h.id));
        o.insert("hop_limit".into(), json!(h.hop_limit));
        o.insert("hop_start".into(), json!(h.hop_start));
        o.insert("hops_taken".into(), json!(h.hops_taken()));
        o.insert("want_ack".into(), json!(h.want_ack));
        o.insert("via_mqtt".into(), json!(h.via_mqtt));
        o.insert("channel_hash".into(), json!(h.channel_hash));
        o.insert("next_hop".into(), json!(h.next_hop));
        o.insert("relay_node".into(), json!(h.relay_node));
    }
    if let Some(c) = &d.channel {
        o.insert("channel".into(), json!(c));
    }
    if let Some(p) = &d.port {
        o.insert("port".into(), json!(p));
    }
    if let Some(x) = &d.data {
        o.insert("data".into(), x.clone());
    }
    if let Some(e) = &d.error {
        o.insert("error".into(), json!(e));
    }
    v
}
